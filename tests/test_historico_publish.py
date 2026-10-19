"""Concurrencia real y fallos controlados; no simula cortes eléctricos."""

from dataclasses import FrozenInstanceError
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal

import pandas as pd
import pytest

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h, ingesta, publicacion as p, versiones


REPO = Path(__file__).resolve().parents[1]


def cargar(ruta):
    return json.loads(ruta.read_bytes())


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
            for ruta in raiz.rglob("*") if ruta.is_file()}


@pytest.fixture(autouse=True)
def referencias_intactas(monkeypatch):
    antes = {ruta: (REPO / ruta).read_bytes() for ruta in h.REFERENCIAS}

    def sin_red(*args, **opciones):
        pytest.fail("No se permite red real")

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", sin_red)
    yield
    assert {ruta: (REPO / ruta).read_bytes() for ruta in antes} == antes


@pytest.fixture
def caso(tmp_path):
    raiz = tmp_path / "repo"
    for ruta in (*h.REFERENCIAS, h.MANIFIESTO_REFERENCIAS):
        archivo = raiz / ruta
        archivo.parent.mkdir(parents=True, exist_ok=True)
        archivo.write_bytes((REPO / ruta).read_bytes())
    store = tmp_path / "store"
    initial = h.inicializar(store, raiz)
    antes = inventario(initial)
    yield {"store": store, "raiz": raiz, "tmp": tmp_path}
    ruta = initial if initial.exists() else store / "work-oculto/initial"
    assert inventario(ruta) == antes


def candidato(caso):
    return h.reconstruir(caso["store"], raiz_repositorio=caso["raiz"])


def publicar(caso, candidato_id=None, resume=None):
    return p.publicar(caso["store"], candidato=candidato_id, resume_version=resume, raiz_repositorio=caso["raiz"])


def identidad_candidato(ruta):
    return "v-" + hashlib.sha256((ruta / "manifest.json").read_bytes()).hexdigest()


def _worker_publicar(store, raiz, candidato_id, barrera, conexion, punto=None):
    """El hijo importa y ejecuta el publicador real, con flock real."""
    try:
        if punto in ("before", "after"):
            reemplazar = p.os.replace

            def reemplazo(origen, destino):
                if Path(destino) == Path(store) / "LATEST" and punto == "before":
                    conexion.send({"event": "before"})
                    conexion.recv()
                reemplazar(origen, destino)
                if Path(destino) == Path(store) / "LATEST" and punto == "after":
                    conexion.send({"event": "after"})
                    conexion.recv()

            p.os.replace = reemplazo
        elif punto == "confirmed":
            sincronizar = p._sincronizar_directorio

            def sincronizacion(ruta):
                sincronizar(ruta)
                if Path(ruta) == Path(store) and (Path(store) / "LATEST").exists():
                    conexion.send({"event": "confirmed"})
                    conexion.recv()

            p._sincronizar_directorio = sincronizacion
        elif punto == "root-pending":
            chmod = Path.chmod

            def sellar(ruta, modo, *args, **opciones):
                if ruta.parent == Path(store) / "versions":
                    conexion.send({"event": "root-pending"})
                    conexion.recv()
                return chmod(ruta, modo, *args, **opciones)

            Path.chmod = sellar
        if barrera is not None:
            barrera.wait(timeout=30)
        resultado = p.publicar(store, candidato=candidato_id, raiz_repositorio=raiz)
        conexion.send({"code": 0, "result": resultado})
    except Exception as error:
        conexion.send({"code": getattr(error, "publication_exit_code", 1), "error": str(error)})
    finally:
        conexion.close()


def _worker_lector(store, ancla, conexion):
    try:
        resuelta = p.resolver_latest(store, ancla)
        sbs_antes = hashlib.sha256(resuelta.sbs.read_bytes()).hexdigest()
        conexion.send({"event": "resolved", "version": resuelta.version_id})
        conexion.recv()
        rutas = [resuelta.sbs, resuelta.bcrp_largo, resuelta.bcrp_ancho, resuelta.features]
        conexion.send({"version": resuelta.version_id, "paths": [str(ruta) for ruta in rutas],
                       "hashes": [hashlib.sha256(ruta.read_bytes()).hexdigest() for ruta in rutas],
                       "sbs_before": sbs_antes})
    finally:
        conexion.close()


def recibir(conexion):
    assert conexion.poll(40), "El proceso no respondió por IPC"
    return conexion.recv()


def terminar(proceso):
    proceso.join(timeout=30)
    if proceso.is_alive():
        proceso.kill()
        proceso.join(timeout=10)
        pytest.fail("Proceso bloqueado")
    assert proceso.exitcode == 0


@pytest.mark.parametrize("con_base", [False, True])
@pytest.mark.parametrize("mismo", [False, True])
def test_dos_publicadores_reales(caso, con_base, mismo):
    if con_base:
        publicar(caso, candidato(caso).name)
    a = candidato(caso)
    b = a if mismo else candidato(caso)
    trabajo = inventario(caso["store"] / "work")
    inode_bloqueo = (caso["store"] / ".publish.lock").stat().st_ino
    ctx = multiprocessing.get_context("spawn")
    barrera = ctx.Barrier(3)
    procesos, conexiones = [], []
    try:
        for origen in (a, b):
            padre, hijo = ctx.Pipe()
            proceso = ctx.Process(target=_worker_publicar, args=(caso["store"], caso["raiz"], origen.name, barrera, hijo))
            proceso.start()
            hijo.close()
            procesos.append(proceso)
            conexiones.append(padre)
        barrera.wait(timeout=30)
        resultados = [recibir(conexion) for conexion in conexiones]
        for proceso in procesos:
            terminar(proceso)
    finally:
        for proceso in procesos:
            if proceso.is_alive():
                proceso.kill()
                proceso.join(timeout=10)
        for conexion in conexiones:
            conexion.close()
    assert sorted(r["code"] for r in resultados) == ([0, 0] if mismo else [0, 3])
    if mismo:
        assert sorted(r["result"]["already_current"] for r in resultados) == [False, True]
    versiones_creadas = list((caso["store"] / "versions").iterdir())
    assert len(versiones_creadas) == 1 + int(con_base)
    assert inventario(caso["store"] / "work") == trabajo
    assert (caso["store"] / ".publish.lock").stat().st_ino == inode_bloqueo
    resuelta = p.resolver_latest(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    assert resuelta.version_id in {identidad_candidato(a), identidad_candidato(b)}
    assert cargar(caso["store"] / "LATEST")["version_id"] == resuelta.version_id


def candidato_con_ingesta_equivalente(caso, monkeypatch):
    carpeta = caso["tmp"] / "excel"
    carpeta.mkdir()
    for nombre in ("B-2362-2023-01.xls", "B-2334-2023-01.xls", "Tasas-2023-01.xlsx"):
        (carpeta / nombre).write_bytes(b"controlado")
    sbs = pd.read_csv(caso["raiz"] / h.SBS, dtype=str)

    def simular(indicador):
        def extraer(ruta, mes):
            datos = sbs[sbs.fecha_id.eq(mes) & sbs.indicador_id.eq(indicador)].copy()
            mascara = datos.banco_id.eq("Total Banca Múltiple")
            datos.loc[mascara, "valor"] += "0"
            return datos
        return extraer

    for nombre, indicador in (("extraer_morosidad", "Morosidad"), ("extraer_creditos_directos", "Créditos Directos Totales"),
                              ("extraer_tasas_activas", "Tasa Activa MN")):
        monkeypatch.setattr(sbs_extractor, nombre, simular(indicador))

    def bcrp(periodo_ini, periodo_fin, directorio_salida):
        assert periodo_ini == periodo_fin == "2023-01"
        largo = pd.read_csv(caso["raiz"] / h.BCRP, dtype=str)
        largo = largo[largo.fecha_id.eq(periodo_ini)].copy()
        largo.loc[largo.indicador_id.eq("tasa_referencia"), "valor"] = "7.750"
        ancho = largo.pivot(index="fecha_id", columns="indicador_id", values="valor").reset_index()
        directorio_salida.mkdir(parents=True)
        largo.to_csv(directorio_salida / "bcrp_macro_2023_2025.csv", index=False)
        ancho[["fecha_id", "tipo_cambio", "tasa_referencia", "inflacion"]].to_csv(
            directorio_salida / "macro_bcrp_2023_2025.csv", index=False)
        return ancho

    monkeypatch.setattr(bcrp_extractor, "extraer_bcrp_api", bcrp)
    origen = ingesta.ingerir(caso["store"], "initial", "2023-01", carpeta, caso["raiz"])
    return h.reconstruir(caso["store"], candidato=origen.name, raiz_repositorio=caso["raiz"])


def test_lector_real_conserva_a_mientras_se_publica_b(caso, monkeypatch):
    a = publicar(caso, candidato(caso).name)
    b = candidato_con_ingesta_equivalente(caso, monkeypatch)
    ctx = multiprocessing.get_context("spawn")
    padre, hijo = ctx.Pipe()
    lector = ctx.Process(target=_worker_lector, args=(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS, hijo))
    lector.start()
    hijo.close()
    try:
        assert recibir(padre) == {"event": "resolved", "version": a["version_id"]}
        resultado_b = publicar(caso, b.name)
        padre.send("continuar")
        observado = recibir(padre)
        terminar(lector)
    finally:
        if lector.is_alive():
            lector.kill()
            lector.join(timeout=10)
        padre.close()
    assert observado["version"] == a["version_id"]
    raiz_a = Path(a["path"])
    assert all(Path(ruta).is_relative_to(raiz_a) for ruta in observado["paths"])
    assert observado["hashes"][0] == observado["sbs_before"]
    nueva = p.resolver_latest(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    assert nueva.version_id == resultado_b["version_id"] != a["version_id"]
    assert hashlib.sha256(nueva.sbs.read_bytes()).hexdigest() != observado["hashes"][0]
    assert hashlib.sha256(nueva.bcrp_largo.read_bytes()).hexdigest() != observado["hashes"][1]
    with pytest.raises(FrozenInstanceError):
        nueva.version_id = "otra"


@pytest.mark.parametrize("punto", ["before", "after", "confirmed"])
def test_terminacion_real_y_reanudacion_explicita(caso, punto):
    origen = candidato(caso)
    ctx = multiprocessing.get_context("spawn")
    padre, hijo = ctx.Pipe()
    proceso = ctx.Process(target=_worker_publicar, args=(caso["store"], caso["raiz"], origen.name, None, hijo, punto))
    proceso.start()
    hijo.close()
    try:
        assert recibir(padre) == {"event": punto}
        os.kill(proceso.pid, signal.SIGKILL)
        proceso.join(timeout=10)
        assert proceso.exitcode == -signal.SIGKILL
    finally:
        if proceso.is_alive():
            proceso.kill()
            proceso.join(timeout=10)
        padre.close()
    identificador = identidad_candidato(origen)
    versiones.verificar_version(caso["store"], identificador, caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    assert (caso["store"] / "LATEST").exists() == (punto != "before")
    temporales = {ruta.name for ruta in caso["store"].iterdir() if ruta.name.startswith((".publish-", ".LATEST-"))}
    assert temporales
    resultado = publicar(caso, resume=identificador)
    assert resultado["already_current"] == (punto != "before")
    assert temporales <= {ruta.name for ruta in caso["store"].iterdir()}
    assert len(list((caso["store"] / "versions").iterdir())) == 1


@pytest.mark.parametrize("fallo, codigo", [("fsync-previo", 1), ("fsync-temp", 1), ("fsync-directorio", 4),
                                          ("replace-previo", 1), ("replace-confirmacion", 4)])
def test_fallos_conservan_excepcion_codigo_y_evidencia(caso, monkeypatch, fallo, codigo):
    origen = candidato(caso)
    error = OSError("fallo controlado")
    fsync, syncdir, replace = p.os.fsync, p._sincronizar_directorio, p.os.replace

    def sincronizar(fd):
        ruta = os.readlink(f"/proc/self/fd/{fd}")
        if fallo == "fsync-temp" and Path(ruta).name.startswith(".LATEST-"):
            raise error
        return fsync(fd)

    def directorio(ruta):
        if Path(ruta) == caso["store"] and (fallo == "fsync-previo" or
                (fallo == "fsync-directorio" and (caso["store"] / "LATEST").exists())):
            raise error
        return syncdir(ruta)

    def reemplazar(a, b):
        if fallo == "replace-previo":
            raise error
        replace(a, b)
        if fallo == "replace-confirmacion":
            raise error

    with monkeypatch.context() as cambio:
        cambio.setattr(p.os, "fsync", sincronizar)
        cambio.setattr(p, "_sincronizar_directorio", directorio)
        cambio.setattr(p.os, "replace", reemplazar)
        with pytest.raises(OSError) as capturado:
            publicar(caso, origen.name)
        assert capturado.value is error
        assert error.publication_exit_code == codigo
    assert (caso["store"] / "LATEST").exists() == (codigo == 4)
    diagnosticos = list(caso["store"].glob(".publication-error-*.json"))
    assert len(diagnosticos) == 1 and cargar(diagnosticos[0])["exit_code"] == codigo
    if fallo in ("fsync-temp", "replace-previo"):
        assert list(caso["store"].glob(".LATEST-*.tmp"))
    resultado = publicar(caso, origen.name)
    assert resultado["already_current"] == (codigo == 4)
    assert len(list((caso["store"] / "versions").iterdir())) == 1


def test_huerfana_no_reactiva_base_obsoleta(caso, monkeypatch):
    a = candidato(caso)
    b = candidato(caso)

    def fallar(*args):
        raise OSError("sin reemplazo")

    with monkeypatch.context() as cambio:
        cambio.setattr(p.os, "replace", fallar)
        with pytest.raises(OSError):
            publicar(caso, a.name)
    publicar(caso, b.name)
    antes = (caso["store"] / "LATEST").read_bytes()
    for opciones in ({"resume": identidad_candidato(a)}, {"candidato_id": a.name}):
        with pytest.raises(p.BaseObsoleta) as error:
            publicar(caso, **opciones)
        assert error.value.publication_exit_code == 3
        assert (caso["store"] / "LATEST").read_bytes() == antes


@pytest.mark.parametrize("defecto", ["latest-json", "latest-enlace", "versions-archivo", "destino-vacio", "destino-enlace", "destino-invalido"])
def test_destinos_y_punteros_invalidos_no_se_sobrescriben(caso, defecto):
    origen = candidato(caso)
    contenedor = caso["store"] / "versions"
    destino = contenedor / identidad_candidato(origen)
    latest = caso["store"] / "LATEST"
    if defecto == "latest-json":
        latest.write_bytes(b"{")
    elif defecto == "latest-enlace":
        latest.symlink_to(caso["tmp"] / "inexistente")
    elif defecto == "versions-archivo":
        contenedor.write_bytes(b"intacto")
    else:
        contenedor.mkdir()
        if defecto == "destino-enlace":
            destino.symlink_to(origen, target_is_directory=True)
        else:
            destino.mkdir()
            if defecto == "destino-invalido":
                (destino / "manifest.json").write_bytes(b"{}")
    conector = latest if defecto.startswith("latest") else contenedor if defecto == "versions-archivo" else destino
    inode = conector.lstat().st_ino
    with pytest.raises((ValueError, OSError)) as error:
        publicar(caso, origen.name)
    assert getattr(error.value, "publication_exit_code", 1) == 1
    assert conector.lstat().st_ino == inode


def test_filesystem_incompatible_y_dispositivo_distinto(caso, monkeypatch):
    origen = candidato(caso)
    with monkeypatch.context() as cambio:
        cambio.setattr(p, "_tipo_filesystem", lambda ruta: "nfs4")
        with pytest.raises(ValueError, match="Filesystem no admitido"):
            publicar(caso, origen.name)
    destino = caso["store"] / "versions"
    destino.mkdir()
    original = Path.stat

    def stat_alterado(ruta, *args, **opciones):
        resultado = original(ruta, *args, **opciones)
        if ruta == destino and opciones.get("follow_symlinks", True):
            campos = list(resultado)
            campos[2] += 1
            return os.stat_result(campos)
        return resultado

    with monkeypatch.context() as cambio:
        cambio.setattr(Path, "stat", stat_alterado)
        with pytest.raises(ValueError, match="compartir filesystem"):
            publicar(caso, origen.name)
    assert not (caso["store"] / "LATEST").exists()


def test_cli_selectores_codigos_resolve_y_verify(caso, capsys):
    a, b = candidato(caso), candidato(caso)
    argumentos = ["--store", str(caso["store"])]
    for opciones in (["publish", *argumentos], ["publish", *argumentos, "--candidate", a.name, "--resume-version", "v-x"],
                     ["verify", *argumentos, "--candidate", "initial", "--version", "v-x"],
                     ["publish", *argumentos, "--candidate", a.name, "--force"]):
        with pytest.raises(SystemExit) as salida:
            h.main(opciones)
        assert salida.value.code == 2
    assert h.main(["resolve", *argumentos]) == 1
    assert h.main(["publish", *argumentos, "--candidate", a.name]) == 0
    assert h.main(["publish", *argumentos, "--candidate", b.name]) == 3
    assert h.main(["publish", *argumentos, "--candidate", a.name]) == 0
    assert h.main(["resolve", *argumentos]) == 0
    assert h.main(["verify", *argumentos, "--version", identidad_candidato(a)]) == 0
    assert h.main(["verify", *argumentos]) == 0
    assert "Versión verificada" in capsys.readouterr().out


def test_lector_una_lectura_y_reanudacion_sin_work_ni_datos(caso, monkeypatch):
    resultado = publicar(caso, candidato(caso).name)
    for ruta in Path(resultado["path"]).rglob("*"):
        assert ruta.stat().st_mode & 0o222 == 0
    assert Path(resultado["path"]).stat().st_mode & 0o222 == 0
    (caso["store"] / "work").rename(caso["store"] / "work-oculto")
    (caso["raiz"] / "data").rename(caso["tmp"] / "datos-ocultos")
    abrir = os.open
    lecturas = []

    def contar(ruta, *args, **opciones):
        if Path(ruta) == caso["store"] / "LATEST":
            lecturas.append(ruta)
        return abrir(ruta, *args, **opciones)

    def prohibido(*args, **opciones):
        pytest.fail("No puede consultar work/ ni datos originales")

    monkeypatch.setattr(h, "_leer_referencias", prohibido)
    monkeypatch.setattr(h, "verificar", prohibido)
    monkeypatch.setattr(os, "open", contar)
    resuelta = p.resolver_latest(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    for ruta in (resuelta.sbs, resuelta.bcrp_largo, resuelta.bcrp_ancho, resuelta.features):
        assert ruta.read_bytes()
    assert len(lecturas) == 1
    assert publicar(caso, resume=resultado["version_id"])["already_current"] is True


def test_version_antes_actual_no_se_reactiva(caso):
    a = publicar(caso, candidato(caso).name)
    b = publicar(caso, candidato(caso).name)
    antes = (caso["store"] / "LATEST").read_bytes()
    with pytest.raises(p.BaseObsoleta):
        publicar(caso, resume=a["version_id"])
    assert (caso["store"] / "LATEST").read_bytes() == antes
    assert p.resolver_latest(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS).version_id == b["version_id"]


@pytest.mark.parametrize("tipo", ["initial", "legacy", "ingest"])
def test_solo_rebuild_con_contexto_puede_publicarse(caso, monkeypatch, tipo):
    if tipo == "initial":
        identificador = "initial"
    elif tipo == "legacy":
        origen = candidato(caso)
        datos = cargar(origen / "manifest.json")
        datos["schema_version"] = 1
        datos.pop("publication_base")
        datos.pop("publication_context")
        contexto = "audit/publication-base.json"
        datos["artifacts"].pop(contexto)
        (origen / contexto).unlink()
        (origen / "manifest.json").write_text(json.dumps(datos), encoding="utf-8")
        identificador = origen.name
    else:
        reconstruido = candidato_con_ingesta_equivalente(caso, monkeypatch)
        identificador = cargar(reconstruido / "manifest.json")["source_candidate"]["id"]
    h.verificar(caso["store"], caso["raiz"], candidato=identificador)
    with pytest.raises(ValueError, match="Solo se publican"):
        publicar(caso, identificador)
    assert not (caso["store"] / "LATEST").exists()
    assert not (caso["store"] / "versions").exists()


def test_cli_devuelve_cuatro_si_falla_confirmacion(caso, monkeypatch, capsys):
    origen = candidato(caso)
    sincronizar = p._sincronizar_directorio

    def fallar(ruta):
        if Path(ruta) == caso["store"] and (caso["store"] / "LATEST").exists():
            raise OSError("durabilidad sin confirmar")
        return sincronizar(ruta)

    argumentos = ["--store", str(caso["store"])]
    with monkeypatch.context() as cambio:
        cambio.setattr(p, "_sincronizar_directorio", fallar)
        assert h.main(["publish", *argumentos, "--candidate", origen.name]) == 4
    assert "durabilidad sin confirmar" in capsys.readouterr().err
    assert h.main(["publish", *argumentos, "--resume-version", identidad_candidato(origen)]) == 0


def test_fallo_chmod_despues_de_rename_requiere_finalizacion_explicita(caso, monkeypatch):
    anterior = publicar(caso, candidato(caso).name)
    origen = candidato(caso)
    destino = caso["store"] / "versions" / identidad_candidato(origen)
    latest = (caso["store"] / "LATEST").read_bytes()
    chmod = Path.chmod
    fallo = OSError("fallo chmod después de rename")

    def fallar(ruta, modo, *args, **opciones):
        if ruta == destino:
            raise fallo
        return chmod(ruta, modo, *args, **opciones)

    with monkeypatch.context() as cambio:
        cambio.setattr(Path, "chmod", fallar)
        with pytest.raises(OSError) as capturado:
            publicar(caso, origen.name)
        assert capturado.value is fallo
    assert destino.stat().st_mode & 0o777 == 0o755
    assert (caso["store"] / "LATEST").read_bytes() == latest
    with pytest.raises(ValueError, match="solo lectura"):
        publicar(caso, origen.name)
    assert (caso["store"] / "LATEST").read_bytes() == latest
    assert cargar(caso["store"] / "LATEST")["version_id"] == anterior["version_id"]
    contenido = inventario(destino)
    inode = destino.stat().st_ino
    resultado = publicar(caso, resume=destino.name)
    assert resultado["already_current"] is False
    assert destino.stat().st_ino == inode
    assert destino.stat().st_mode & 0o777 == 0o555
    assert inventario(destino) == contenido
    assert publicar(caso, resume=destino.name)["already_current"] is True
    assert len(list((caso["store"] / "versions").iterdir())) == 2


@pytest.mark.parametrize("defecto", ["contenido", "archivo-escribible", "directorio-escribible",
                                    "archivo-sin-lectura", "raiz", "constancia", "inode", "base-obsoleta"])
def test_finalizacion_no_repara_paquetes_alterados(caso, monkeypatch, defecto):
    origen = candidato(caso)
    alternativo = candidato(caso) if defecto == "base-obsoleta" else None
    destino = caso["store"] / "versions" / identidad_candidato(origen)
    chmod = Path.chmod

    def fallar(ruta, modo, *args, **opciones):
        if ruta == destino:
            raise OSError("raíz pendiente")
        return chmod(ruta, modo, *args, **opciones)

    with monkeypatch.context() as cambio:
        cambio.setattr(Path, "chmod", fallar)
        with pytest.raises(OSError):
            publicar(caso, origen.name)
    registro = next(caso["store"].glob(".publish-*/finalization.json"))
    archivo = destino / "manifest.json"
    if defecto == "contenido":
        archivo.chmod(0o644)
        archivo.write_bytes(b"{}")
        archivo.chmod(0o444)
    elif defecto in ("archivo-escribible", "archivo-sin-lectura"):
        archivo.chmod(0o644 if defecto == "archivo-escribible" else 0o400)
    elif defecto == "directorio-escribible":
        (destino / "payload").chmod(0o755)
    elif defecto == "raiz":
        destino.chmod(0o775)
    elif defecto == "constancia":
        registro.unlink()
    elif defecto == "inode":
        datos = cargar(registro)
        datos["inode"] += 1
        registro.write_text(json.dumps(datos))
    else:
        publicar(caso, alternativo.name)
    latest = caso["store"] / "LATEST"
    antes = latest.read_bytes() if latest.exists() else None
    contenido = inventario(destino)
    permisos = {ruta: ruta.stat().st_mode for ruta in [destino, *destino.rglob("*")]}
    with pytest.raises(ValueError):
        publicar(caso, resume=destino.name)
    assert (latest.read_bytes() if latest.exists() else None) == antes
    assert inventario(destino) == contenido
    assert {ruta: ruta.stat().st_mode for ruta in permisos} == permisos


@pytest.mark.parametrize("puntero_ausente", [False, True])
def test_version_actual_con_raiz_alterada_no_se_repara(caso, puntero_ausente):
    resultado = publicar(caso, candidato(caso).name)
    destino = Path(resultado["path"])
    antes = (caso["store"] / "LATEST").read_bytes()
    destino.chmod(0o755)
    if puntero_ausente:
        (caso["store"] / "LATEST").unlink()
    with pytest.raises(ValueError, match="no se reparan|solo lectura"):
        publicar(caso, resume=destino.name)
    assert destino.stat().st_mode & 0o777 == 0o755
    if puntero_ausente:
        assert not (caso["store"] / "LATEST").exists()
    else:
        assert (caso["store"] / "LATEST").read_bytes() == antes


def test_terminacion_real_tras_rename_antes_de_chmod(caso):
    publicar(caso, candidato(caso).name)
    origen = candidato(caso)
    antes = (caso["store"] / "LATEST").read_bytes()
    ctx = multiprocessing.get_context("spawn")
    padre, hijo = ctx.Pipe()
    proceso = ctx.Process(target=_worker_publicar,
                          args=(caso["store"], caso["raiz"], origen.name, None, hijo, "root-pending"))
    proceso.start()
    hijo.close()
    try:
        assert recibir(padre) == {"event": "root-pending"}
        proceso.kill()
        proceso.join(timeout=10)
        assert proceso.exitcode == -signal.SIGKILL
    finally:
        if proceso.is_alive():
            proceso.kill()
            proceso.join(timeout=10)
        padre.close()
    destino = caso["store"] / "versions" / identidad_candidato(origen)
    assert destino.stat().st_mode & 0o777 == 0o755
    assert (caso["store"] / "LATEST").read_bytes() == antes
    antes_paquete = inventario(destino)
    resultado = publicar(caso, resume=destino.name)
    assert resultado["already_current"] is False
    assert destino.stat().st_mode & 0o777 == 0o555
    assert inventario(destino) == antes_paquete
    assert publicar(caso, resume=destino.name)["already_current"] is True
    assert len(list((caso["store"] / "versions").iterdir())) == 2
