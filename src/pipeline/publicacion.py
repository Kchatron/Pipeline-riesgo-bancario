"""Publicación local Linux: versiones inmutables y cambio atómico de LATEST."""

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
from uuid import uuid4

from src.pipeline import historico as h, versiones
from src.pipeline.contexto_publicacion import bloqueo_publicacion, campos_contexto, leer_json, leer_latest


class BaseObsoleta(ValueError):
    publication_exit_code = 3


@dataclass(frozen=True)
class VersionResuelta:
    version_id: str
    manifest_sha256: str
    root: Path
    sbs: Path
    bcrp_largo: Path
    bcrp_ancho: Path
    features: Path

    def como_dict(self):
        return {campo: str(getattr(self, campo)) for campo in self.__dataclass_fields__}


def resolver_latest(store, ancla=h.RAIZ_REPOSITORIO / h.MANIFIESTO_REFERENCIAS):
    """Una sola lectura de LATEST; todas las rutas quedan ligadas a esa versión."""
    store = Path(store).resolve()
    identidad = leer_latest(store, ancla)
    if identidad is None:
        raise ValueError("LATEST no existe; no hay una versión actual para resolver.")
    raiz = versiones.ruta_version(store, identidad["version_id"])
    payload = raiz / versiones.PAYLOAD
    return VersionResuelta(identidad["version_id"], identidad["manifest_sha256"], raiz,
                           payload / h.RECOMPUESTOS[h.SBS], payload / h.RECOMPUESTOS[h.BCRP],
                           payload / h.RECOMPUESTOS[h.MACRO], payload / h.FEATURES_RECONSTRUIDAS)


def _sincronizar_directorio(ruta):
    descriptor = os.open(ruta, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _tipo_filesystem(ruta):
    if sys.platform != "linux":
        raise ValueError("La publicación está limitada a filesystem local Linux.")
    montajes = []
    for linea in Path("/proc/self/mountinfo").read_text().splitlines():
        izquierda, derecha = linea.split(" - ", 1)
        punto = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), izquierda.split()[4])
        if ruta.is_relative_to(Path(punto)):
            montajes.append((len(Path(punto).parts), derecha.split()[0]))
    if not montajes:
        raise ValueError("No se pudo identificar el filesystem del almacén.")
    return max(montajes)[1]


def _comprobar_filesystem(store, *rutas):
    # Lista explícita del alcance local; no se asume semántica POSIX de montajes remotos.
    permitidos = {"ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs", "overlay", "zfs"}
    if _tipo_filesystem(store) not in permitidos:
        raise ValueError("Filesystem no admitido por el contrato local de publicación.")
    dispositivo = store.stat().st_dev
    for ruta in rutas:
        versiones._sin_enlaces(ruta)
        if ruta.stat().st_dev != dispositivo:
            raise ValueError("El almacén, los temporales y versions/ deben compartir filesystem.")


def _sincronizar_paquete(paquete, dispositivo, sellar=False, raiz_movible=False):
    rutas = [*paquete.rglob("*"), paquete]
    for ruta in rutas:
        versiones._sin_enlaces(ruta)
        modo = ruta.stat()
        if modo.st_dev != dispositivo or not (stat.S_ISREG(modo.st_mode) or stat.S_ISDIR(modo.st_mode)):
            raise ValueError("Artefacto o filesystem incompatible dentro de la versión.")
        if not sellar and modo.st_mode & 0o222:
            raise ValueError("Una versión existente debe estar finalizada en modo de solo lectura; no se repara automáticamente.")
    for ruta in rutas:
        if ruta.is_file():
            if sellar:
                ruta.chmod(0o444)
            descriptor = os.open(ruta, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    for ruta in sorted((ruta for ruta in rutas if ruta.is_dir()), key=lambda p: len(p.parts), reverse=True):
        if sellar:
            # La raíz necesita escritura para mover su '..' a otro padre en Linux.
            ruta.chmod(0o755 if raiz_movible and ruta == paquete else 0o555)
        _sincronizar_directorio(ruta)


def _actualizar_latest(store, identidad, ancla, estado):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=store,
                                     prefix=".LATEST-", suffix=".tmp", delete=False) as archivo:
        estado["latest_temp"] = archivo.name
        json.dump(identidad, archivo, sort_keys=True, allow_nan=False)
        archivo.write("\n")
        archivo.flush()
        os.fsync(archivo.fileno())
    estado["phase"] = "replace-latest"
    try:
        os.replace(estado["latest_temp"], store / "LATEST")
    except OSError as error:
        # Conserva la excepción original, incluso si el syscall perdió su confirmación.
        try:
            estado["visible"] = leer_latest(store, ancla) == identidad
        except Exception as inspeccion:
            estado["uncertain"] = True
            error.add_note(f"No se pudo confirmar el resultado del reemplazo: {inspeccion}")
        raise
    estado["visible"] = True
    estado["phase"] = "latest-visible"
    _sincronizar_directorio(store)
    estado["phase"] = "durable"


def _identidad_finalizacion(paquete, identificador, base):
    datos = paquete.stat()
    return {"schema_version": 1, "version_id": identificador, "publication_base": base,
            "manifest_sha256": h.sha256((paquete / "manifest.json").read_bytes()),
            "device": datos.st_dev, "inode": datos.st_ino}


def _registrar_finalizacion(temporal, paquete, identificador, base):
    """Constancia previa al rename; queda fuera del paquete y no se elimina."""
    with (temporal / "finalization.json").open("x", encoding="utf-8") as archivo:
        json.dump(_identidad_finalizacion(paquete, identificador, base), archivo,
                  sort_keys=True, allow_nan=False)
        archivo.write("\n")
        archivo.flush()
        os.fsync(archivo.fileno())
    _sincronizar_directorio(temporal)


def _cerrar_finalizacion(temporal):
    # Una raíz ya sellada no vuelve a ser reparable si alguien altera sus permisos.
    cierre = temporal / "finalized"
    versiones._sin_enlaces(cierre)
    if cierre.exists():
        if not cierre.is_file() or cierre.read_bytes() != b"":
            raise ValueError("Constancia de cierre inválida.")
        with cierre.open("rb") as archivo:
            os.fsync(archivo.fileno())
        _sincronizar_directorio(temporal)
        return
    with cierre.open("xb") as archivo:
        archivo.flush()
        os.fsync(archivo.fileno())
    _sincronizar_directorio(temporal)


def _constancias_finalizacion(store, destino, base):
    esperado = _identidad_finalizacion(destino, destino.name, base)
    encontrados = []
    for temporal in store.glob(".publish-*"):
        registro = temporal / "finalization.json"
        versiones._sin_enlaces(registro)
        if registro.is_file() and leer_json(registro.read_bytes()) == esperado:
            encontrados.append(temporal)
    return encontrados


def _completar_raiz_pendiente(store, destino, base, ancla):
    """Solo finaliza una raíz 0755 huérfana con constancia del mismo inode."""
    versiones.verificar_version(store, destino.name, ancla)
    if stat.S_IMODE(destino.stat().st_mode) != 0o755:
        raise ValueError("La raíz pendiente debe tener exactamente permisos 0755.")
    for ruta in destino.rglob("*"):
        versiones._sin_enlaces(ruta)
        datos = ruta.stat()
        esperado = 0o555 if stat.S_ISDIR(datos.st_mode) else 0o444
        if (not (stat.S_ISDIR(datos.st_mode) or stat.S_ISREG(datos.st_mode)) or
                stat.S_IMODE(datos.st_mode) != esperado or datos.st_dev != destino.stat().st_dev):
            raise ValueError("Permisos internos inválidos: no se reparan.")
    encontrados = _constancias_finalizacion(store, destino, base)
    if len(encontrados) != 1:
        raise ValueError("No existe constancia de finalización para este inode y manifiesto.")
    cierre = encontrados[0] / "finalized"
    if cierre.exists() or cierre.is_symlink():
        raise ValueError("La raíz ya fue finalizada: no se reparan permisos alterados.")
    destino.chmod(0o555)
    _sincronizar_directorio(destino)
    _cerrar_finalizacion(encontrados[0])


def _diagnosticar(store, error, estado):
    estado["error_type"] = type(error).__name__
    estado["error"] = str(error)
    codigo = 4 if estado["visible"] or estado["uncertain"] else getattr(error, "publication_exit_code", 1)
    error.publication_exit_code = codigo
    estado["exit_code"] = codigo
    ruta = store / f".publication-error-{uuid4().hex}.json"
    error.add_note(f"Publicación: fase={estado['phase']}, código={codigo}, versión={estado.get('version_id')}; no se revierte LATEST.")
    try:
        with ruta.open("x", encoding="utf-8") as archivo:
            json.dump(estado, archivo, indent=2, ensure_ascii=False, allow_nan=False)
            archivo.write("\n")
        error.add_note(f"Diagnóstico: {ruta}")
    except Exception as fallo:
        error.add_note(f"No se pudo escribir diagnóstico adicional: {fallo}")


def publicar(store, candidato=None, resume_version=None, raiz_repositorio=h.RAIZ_REPOSITORIO):
    """Publica o reanuda explícitamente; nunca borra versiones o temporales."""
    if (candidato is None) == (resume_version is None):
        raise ValueError("Seleccione exactamente un candidato o --resume-version.")
    raiz = Path(raiz_repositorio).resolve()
    store = h._destino(store, raiz)
    ancla = raiz / h.MANIFIESTO_REFERENCIAS
    estado = {"stage": "historical-publication", "phase": "start", "visible": False, "uncertain": False}
    try:
        with bloqueo_publicacion(store, exclusivo=True) as bloqueo:
            _comprobar_filesystem(store)
            if os.fstat(bloqueo).st_dev != store.stat().st_dev:
                raise ValueError("El bloqueo no pertenece al filesystem del almacén.")
            os.fsync(bloqueo)
            _sincronizar_directorio(store)
            destino_versiones = store / "versions"
            if destino_versiones.exists() or destino_versiones.is_symlink():
                _comprobar_filesystem(store, destino_versiones)
                if not destino_versiones.is_dir():
                    raise ValueError("versions/ debe ser un directorio.")
            actual = leer_latest(store, ancla)
            if candidato is not None:
                preparado = h.verificar(store, raiz, candidato=candidato)
                if preparado["stage"] != "historical-rebuild" or preparado["schema_version"] != 2:
                    raise ValueError("Solo se publican reconstrucciones verificadas con contexto capturado.")
                origen = h._ruta_candidato(store, candidato)
                contenido = (origen / "manifest.json").read_bytes()
                if leer_json(contenido) != preparado:
                    raise ValueError("El candidato cambió después de verificarlo.")
                identificador = f"v-{h.sha256(contenido)}"
                base = campos_contexto(origen, 2)["publication_base"]
            else:
                identificador = resume_version
                preparado = versiones.verificar_version(store, identificador, ancla)
                base = preparado["publication_base"]
            estado.update(version_id=identificador, expected_base=base, observed_base=actual)
            destino = versiones.ruta_version(store, identificador)
            existente = destino.exists() or destino.is_symlink()
            identidad = None
            if existente:
                paquete = versiones.verificar_version(store, identificador, ancla)
                if paquete["publication_base"] != base:
                    raise ValueError("Base del paquete existente diferente del candidato.")
                identidad = {"version_id": identificador,
                             "manifest_sha256": h.sha256((destino / "manifest.json").read_bytes())}
            ya_actual = identidad is not None and actual == identidad
            if not ya_actual and actual != base:
                raise BaseObsoleta(f"Base obsoleta: esperada={base}, actual={actual}, versión={identificador}. No se actualiza la base.")
            if ya_actual:
                estado.update(visible=True, phase="already-current")
            if not existente:
                if candidato is None:
                    raise ValueError("No existe una versión completa para reanudar.")
                estado["phase"] = "prepare-package"
                temporal = Path(tempfile.mkdtemp(prefix=".publish-", dir=store))
                estado["package_temp"] = str(temporal)
                _comprobar_filesystem(store, temporal)
                paquete = versiones._preparar_paquete(store, candidato, temporal / "package", raiz)
                datos = versiones.verificar_paquete(paquete, ancla)
                if datos["version_id"] != identificador or datos["publication_base"] != base:
                    raise ValueError("El paquete no corresponde al candidato y base verificados.")
                _sincronizar_paquete(paquete, store.stat().st_dev, sellar=True, raiz_movible=True)
                _registrar_finalizacion(temporal, paquete, identificador, base)
                _sincronizar_directorio(temporal)
                _sincronizar_directorio(store)
                destino_versiones.mkdir(exist_ok=True)
                _comprobar_filesystem(store, destino_versiones, paquete)
                if destino.exists() or destino.is_symlink():
                    raise ValueError("El destino apareció durante la publicación; no se sobrescribe.")
                estado["phase"] = "finalize-version"
                # Los escritores CLI mantienen el mismo LOCK_EX durante esta comprobación y rename.
                os.rename(paquete, destino)
                destino.chmod(0o555)
                _sincronizar_directorio(destino)
                _cerrar_finalizacion(temporal)
                _sincronizar_directorio(destino_versiones)
                _sincronizar_directorio(temporal)
                _sincronizar_directorio(store)
                identidad = {"version_id": identificador,
                             "manifest_sha256": h.sha256((destino / "manifest.json").read_bytes())}
            else:
                if (resume_version is not None and not ya_actual and
                        destino.stat().st_mode & 0o222):
                    estado["phase"] = "complete-pending-root"
                    _completar_raiz_pendiente(store, destino, base, ancla)
                _sincronizar_paquete(destino, store.stat().st_dev)
                for temporal in _constancias_finalizacion(store, destino, base):
                    _cerrar_finalizacion(temporal)
                _sincronizar_directorio(destino_versiones)
                _sincronizar_directorio(store)
            estado["phase"] = "version-durable"
            if ya_actual:
                descriptor = os.open(store / "LATEST", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                _sincronizar_directorio(store)
            else:
                _actualizar_latest(store, identidad, ancla, estado)
            return {**identidad, "already_current": ya_actual, "path": str(destino)}
    except Exception as error:
        _diagnosticar(store, error, estado)
        raise
