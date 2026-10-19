"""Paquetes privados verificables sin work/; no publica ni modifica LATEST."""

import json
from pathlib import Path, PurePosixPath
import re
import stat

from src.pipeline import historico as h
from src.pipeline.contexto_publicacion import campos_contexto, leer_json
from src.pipeline.ingesta import verificar_ingesta_archivos


MAN_INITIAL = "evidence/manifests/initial.json"
MAN_INGEST = "evidence/manifests/ingest.json"
MAN_REBUILD = "evidence/manifests/rebuild.json"
MAN_REFERENCIAS = "evidence/references-manifest.json"
INITIAL = "evidence/initial"
PAYLOAD = "payload"


def _sin_enlaces(ruta):
    for componente in (ruta.absolute(), *ruta.absolute().parents):
        if componente.is_symlink():
            raise ValueError("Enlace simbólico no permitido en la ruta del paquete.")


def _ruta_confinada(raiz, relativa):
    raiz = Path(raiz)
    _sin_enlaces(raiz)
    if (not isinstance(relativa, str) or relativa in ("", ".") or "\\" in relativa
            or PurePosixPath(relativa).is_absolute() or ".." in PurePosixPath(relativa).parts
            or PurePosixPath(relativa).as_posix() != relativa):
        raise ValueError(f"Ruta fuera del contrato del paquete: {relativa!r}.")
    if raiz.is_symlink():
        raise ValueError("El paquete no puede ser un enlace simbólico.")
    ruta = raiz
    for parte in PurePosixPath(relativa).parts:
        ruta = ruta / parte
        if ruta.is_symlink():
            raise ValueError(f"Enlace simbólico no permitido en el paquete: {relativa}.")
    if not ruta.resolve().is_relative_to(raiz.resolve()):
        raise ValueError("Ruta fuera del paquete.")
    return ruta


def archivo_paquete(raiz, relativa):
    ruta = _ruta_confinada(raiz, relativa)
    if not stat.S_ISREG(ruta.stat().st_mode):
        raise ValueError(f"Artefacto no regular en el paquete: {relativa}.")
    return ruta


def _inventario(raiz):
    if raiz.is_symlink() or not raiz.is_dir():
        raise ValueError("Se requiere un directorio de paquete sin enlaces.")
    resultado = {}
    for ruta in raiz.rglob("*"):
        relativa = ruta.relative_to(raiz).as_posix()
        _ruta_confinada(raiz, relativa)
        if ruta.is_dir():
            continue
        archivo_paquete(raiz, relativa)
        if relativa != "manifest.json":
            resultado[relativa] = h.sha256(ruta.read_bytes())
    return dict(sorted(resultado.items()))


def _id_candidato(valor):
    if not isinstance(valor, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", valor):
        raise ValueError("Identificador de candidato archivado inválido.")
    return valor


def _manifiesto(paquete, rebuild, rebuild_bytes, initial_bytes, ingest, ingest_bytes, ancla, inventario):
    genealogia = [{"id": "initial", "stage": "historical-init", "manifest_path": MAN_INITIAL,
                  "manifest_sha256": h.sha256(initial_bytes), "artifacts_root": INITIAL}]
    if ingest is not None:
        genealogia.append({"id": ingest["candidate"], "stage": "historical-ingest", "manifest_path": MAN_INGEST,
                           "manifest_sha256": h.sha256(ingest_bytes), "artifacts_root": PAYLOAD})
    return {
        "schema_version": 1, "stage": "historical-version", "status": "complete",
        "version_id": f"v-{h.sha256(rebuild_bytes)}",
        "publication_base": campos_contexto(paquete / PAYLOAD, 2)["publication_base"],
        "source_rebuild": {"candidate_id": rebuild["candidate"], "manifest_path": MAN_REBUILD,
                           "manifest_sha256": h.sha256(rebuild_bytes)},
        "lineage": genealogia, "reference_manifest": {"path": MAN_REFERENCIAS, "sha256": h.sha256(ancla)},
        "payload_root": PAYLOAD, "artifacts": inventario,
        "reference_source_commit": h.COMMIT_REFERENCIAS, "executed_code_commit": None,
    }


def _comprobar(paquete, manifiesto, ancla):
    inventario = _inventario(paquete)
    archivada = archivo_paquete(paquete, MAN_REFERENCIAS).read_bytes()
    if archivada != ancla:
        raise ValueError("Ancla de referencias archivada diferente de la configuración aprobada.")
    hashes = h._hashes_referencias(ancla)
    contenidos = {ruta: archivo_paquete(paquete, f"{INITIAL}/references/{ruta}").read_bytes() for ruta in hashes}
    for ruta, contenido in contenidos.items():
        if h.sha256(contenido) != hashes[ruta]:
            raise ValueError(f"Referencia archivada alterada: {ruta}.")
    initial_bytes = archivo_paquete(paquete, MAN_INITIAL).read_bytes()
    initial = leer_json(initial_bytes)
    h._verificar_initial_archivos(paquete / INITIAL, initial, ancla, contenidos)
    rebuild_bytes = archivo_paquete(paquete, MAN_REBUILD).read_bytes()
    rebuild = leer_json(rebuild_bytes)
    if not isinstance(rebuild, dict) or rebuild.get("stage") != "historical-rebuild" or rebuild.get("schema_version") != 2:
        raise ValueError("Un paquete publicable requiere rebuild de esquema 2 con contexto capturado.")
    identificador = _id_candidato(rebuild.get("candidate"))
    if identificador == "initial":
        raise ValueError("Identificador de reconstrucción archivada incorrecto.")
    origen = rebuild.get("source_candidate")
    if not isinstance(origen, dict) or set(origen) != {"id", "manifest_sha256"}:
        raise ValueError("Genealogía de reconstrucción incorrecta.")
    _id_candidato(origen["id"])
    ingest = ingest_bytes = None
    if origen["id"] == "initial":
        directo, directo_bytes = initial, initial_bytes
    else:
        ingest_bytes = archivo_paquete(paquete, MAN_INGEST).read_bytes()
        ingest = leer_json(ingest_bytes)
        if (not isinstance(ingest, dict) or ingest.get("candidate") != origen["id"]
                or ingest.get("candidate") == identificador or ingest.get("stage") != "historical-ingest"
                or ingest.get("schema_version") != 2):
            raise ValueError("Genealogía no admitida: se requiere initial → ingest → rebuild.")
        verificar_ingesta_archivos(paquete / PAYLOAD, ingest, paquete / INITIAL, initial_bytes, ancla, contenidos,
                                   identificador=origen["id"], adicionales=(h.FEATURES_RECONSTRUIDAS, h.AUDITORIA_P75))
        directo, directo_bytes = ingest, ingest_bytes
    h._verificar_rebuild_archivos(paquete / PAYLOAD, rebuild, directo, directo_bytes, ancla, contenidos,
                                  identificador=identificador)
    esperados = ({f"{PAYLOAD}/{ruta}" for ruta in rebuild["artifacts"]}
                 | {f"{INITIAL}/{ruta}" for ruta in initial["artifacts"]}
                 | {MAN_INITIAL, MAN_REBUILD, MAN_REFERENCIAS})
    if ingest is not None:
        esperados.add(MAN_INGEST)
    if set(inventario) != esperados:
        raise ValueError("Inventario del paquete incompleto o con artefactos inesperados.")
    esperado = _manifiesto(paquete, rebuild, rebuild_bytes, initial_bytes, ingest, ingest_bytes, ancla, inventario)
    if manifiesto != esperado:
        raise ValueError("Manifiesto del paquete incorrecto, genealogía o hashes diferentes.")
    return esperado


def verificar_paquete(paquete, ancla=h.RAIZ_REPOSITORIO / h.MANIFIESTO_REFERENCIAS):
    """Solo necesita el paquete, código de validación y el archivo de ancla aprobado."""
    paquete = Path(paquete)
    manifiesto = leer_json(archivo_paquete(paquete, "manifest.json").read_bytes())
    return _comprobar(paquete, manifiesto, Path(ancla).read_bytes())


def ruta_version(store, identificador):
    if not isinstance(identificador, str) or not re.fullmatch(r"v-[0-9a-f]{64}", identificador):
        raise ValueError("Identificador de versión inválido.")
    return _ruta_confinada(Path(store), f"versions/{identificador}")


def verificar_version(store, identificador, ancla=h.RAIZ_REPOSITORIO / h.MANIFIESTO_REFERENCIAS):
    datos = verificar_paquete(ruta_version(store, identificador), ancla)
    if datos["version_id"] != identificador:
        raise ValueError("Nombre de versión diferente del identificador del paquete.")
    return datos


def _preparar_paquete(store, candidato, destino, raiz_repositorio=h.RAIZ_REPOSITORIO):
    """Prepara un directorio privado nuevo; no crea versiones/ ni escribe LATEST."""
    raiz = Path(raiz_repositorio).resolve()
    store = h._destino(store, raiz)
    preparado = h.verificar(store, raiz, candidato=candidato)
    if preparado["stage"] != "historical-rebuild" or preparado["schema_version"] != 2:
        raise ValueError("Se requiere rebuild con contexto; un candidato antiguo no se vuelve publicable retrospectivamente.")
    origen = h._ruta_candidato(store, candidato)
    campos_contexto(origen, 2)
    destino_original = Path(destino)
    _sin_enlaces(destino_original)
    destino = h._destino(destino, raiz)
    if (destino == store or store.is_relative_to(destino) or destino.is_relative_to(store / "work")
            or destino.is_relative_to(store / "versions")):
        raise ValueError("El paquete debe estar separado de work/ y versions/.")
    initial = h._ruta_candidato(store, "initial")
    initial_bytes = (initial / "manifest.json").read_bytes()
    initial_datos = leer_json(initial_bytes)
    rebuild_bytes = (origen / "manifest.json").read_bytes()
    if leer_json(rebuild_bytes) != preparado:
        raise ValueError("El manifiesto rebuild cambió después de verificarlo.")
    directo = h._ruta_candidato(store, preparado["source_candidate"]["id"])
    ingest_bytes = (directo / "manifest.json").read_bytes() if directo != initial else None
    ancla = (raiz / h.MANIFIESTO_REFERENCIAS).read_bytes()
    destino.mkdir(parents=True, exist_ok=False)

    def copiar(relativa, contenido):
        ruta = _ruta_confinada(destino, relativa)
        ruta.parent.mkdir(parents=True, exist_ok=True)
        with ruta.open("xb") as archivo:
            archivo.write(contenido)

    for nombre, fuente, datos in ((PAYLOAD, origen, preparado), (INITIAL, initial, initial_datos)):
        for ruta, huella in datos["artifacts"].items():
            contenido = archivo_paquete(fuente, ruta).read_bytes()
            if h.sha256(contenido) != huella:
                raise ValueError(f"El candidato cambió durante la copia: {ruta}.")
            copiar(f"{nombre}/{ruta}", contenido)
    copiar(MAN_INITIAL, initial_bytes)
    copiar(MAN_REBUILD, rebuild_bytes)
    copiar(MAN_REFERENCIAS, ancla)
    if ingest_bytes is not None:
        copiar(MAN_INGEST, ingest_bytes)
    inventario = _inventario(destino)
    ingest = leer_json(ingest_bytes) if ingest_bytes is not None else None
    final = _manifiesto(destino, preparado, rebuild_bytes, initial_bytes, ingest, ingest_bytes, ancla, inventario)
    _comprobar(destino, final, ancla)
    with (destino / "manifest.json").open("x", encoding="utf-8") as archivo:
        json.dump(final, archivo, indent=2, ensure_ascii=False, sort_keys=True, allow_nan=False)
        archivo.write("\n")
    return destino
