"""Contexto capturado y paquetes privados; todas las escrituras son temporales."""

import fcntl
import hashlib
import io
import json
from pathlib import Path
import shutil

import pandas as pd
import pytest

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h, ingesta, versiones
from src.pipeline.contexto_publicacion import CONTEXTO, capturar_contexto, campos_contexto


REPO = Path(__file__).resolve().parents[1]


def cargar(ruta):
    return json.loads(ruta.read_bytes())


def guardar(ruta, datos):
    ruta.write_text(json.dumps(datos, ensure_ascii=False, allow_nan=False), encoding="utf-8")


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
            for ruta in raiz.rglob("*") if ruta.is_file()}


@pytest.fixture(autouse=True)
def referencias_intactas():
    antes = {ruta: (REPO / ruta).read_bytes() for ruta in h.REFERENCIAS}
    yield
    assert {ruta: (REPO / ruta).read_bytes() for ruta in antes} == antes


@pytest.fixture
def caso(tmp_path, monkeypatch):
    raiz = tmp_path / "repo"
    for ruta in (*h.REFERENCIAS, h.MANIFIESTO_REFERENCIAS):
        destino = raiz / ruta
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes((REPO / ruta).read_bytes())
    store = tmp_path / "store"
    initial = h.inicializar(store, raiz)
    inicial_antes = inventario(initial)
    excel = tmp_path / "excel"
    excel.mkdir()
    for nombre in ("B-2362-2023-01.xls", "B-2334-2023-01.xls", "Tasas-2023-01.xlsx"):
        (excel / nombre).write_bytes(b"entrada controlada")
    sbs = pd.read_csv(raiz / h.SBS, dtype=str, keep_default_na=False)

    def simular(indicador):
        def extraer(ruta, mes):
            assert ruta.is_relative_to(store / "work")
            return sbs[sbs.fecha_id.eq(mes) & sbs.indicador_id.eq(indicador)].copy()
        return extraer

    for nombre, indicador in (("extraer_morosidad", "Morosidad"),
                              ("extraer_creditos_directos", "Créditos Directos Totales"),
                              ("extraer_tasas_activas", "Tasa Activa MN")):
        monkeypatch.setattr(sbs_extractor, nombre, simular(indicador))
    macro = pd.read_csv(raiz / h.BCRP, dtype=str)
    valores = macro[macro.fecha_id.eq("2023-01")].set_index("indicador_id").valor
    payload = {"config": {"series": [{"name": codigo} for codigo in bcrp_extractor.SERIES_BCRP]},
               "periods": [{"name": "Ene.2023", "values": [valores[nombre] for nombre in bcrp_extractor.SERIES_BCRP.values()]}]}
    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", lambda request: io.BytesIO(json.dumps(payload).encode()))
    yield {"raiz": raiz, "store": store, "initial": initial, "excel": excel, "tmp": tmp_path}
    assert inventario(initial if initial.exists() else store / "work-oculto/initial") == inicial_antes


def ingerir(caso):
    return ingesta.ingerir(caso["store"], "initial", "2023-01", caso["excel"], caso["raiz"])


def reconstruir(caso, candidato="initial"):
    return h.reconstruir(caso["store"], candidato=candidato, raiz_repositorio=caso["raiz"])


def empaquetar(caso, candidato, nombre="paquete"):
    return versiones._preparar_paquete(caso["store"], candidato.name, caso["tmp"] / nombre, caso["raiz"])


def latest_controlado(caso):
    """Fixture que representa una versión previa; el código de producción no publica."""
    paquete = empaquetar(caso, reconstruir(caso), "base-privada")
    datos = cargar(paquete / "manifest.json")
    destino = caso["store"] / "versions" / datos["version_id"]
    shutil.copytree(paquete, destino)
    identidad = {"version_id": datos["version_id"], "manifest_sha256": hashlib.sha256((paquete / "manifest.json").read_bytes()).hexdigest()}
    guardar(caso["store"] / "LATEST", identidad)
    return identidad, destino


@pytest.mark.parametrize("origen", ["initial", "ingest"])
def test_paquete_autonomo_determinista_sin_modificar_candidatos(caso, origen):
    directo = caso["initial"] if origen == "initial" else ingerir(caso)
    candidato = reconstruir(caso, directo.name)
    antes = inventario(caso["store"])
    paquete = empaquetar(caso, candidato)
    otro = empaquetar(caso, candidato, "otro-paquete")
    assert inventario(caso["store"]) == antes
    assert (paquete / "manifest.json").read_bytes() == (otro / "manifest.json").read_bytes()
    datos = versiones.verificar_paquete(paquete, caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    assert datos["version_id"] == "v-" + hashlib.sha256((candidato / "manifest.json").read_bytes()).hexdigest()
    assert "manifest.json" not in datos["artifacts"]
    assert versiones.MAN_INITIAL in datos["artifacts"] and versiones.MAN_REBUILD in datos["artifacts"]
    assert datos["artifacts"] == {ruta: huella for ruta, huella in inventario(paquete).items() if ruta != "manifest.json"}
    assert (paquete / versiones.MAN_REBUILD).read_bytes() == (candidato / "manifest.json").read_bytes()
    assert (paquete / versiones.MAN_INITIAL).read_bytes() == (caso["initial"] / "manifest.json").read_bytes()
    assert datos["publication_base"] is None
    assert datos["reference_source_commit"] == h.COMMIT_REFERENCIAS
    assert datos["executed_code_commit"] is None
    assert not (caso["store"] / "LATEST").exists() and not (caso["store"] / "versions").exists()


def test_verificacion_sin_work_datos_originales_ni_red(caso, monkeypatch):
    candidato = reconstruir(caso, ingerir(caso).name)
    paquete = empaquetar(caso, candidato)
    (caso["store"] / "work").rename(caso["store"] / "work-oculto")
    (caso["raiz"] / "data").rename(caso["tmp"] / "datos-inaccesibles")

    def prohibido(*args, **opciones):
        pytest.fail("La verificación del paquete no puede buscar work/, leer datos originales ni ejecutar el pipeline")

    for nombre in ("verificar", "_ruta_candidato", "_leer_referencias", "construir_dataset_analitico"):
        monkeypatch.setattr(h, nombre, prohibido)
    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", prohibido)
    for nombre in ("extraer_morosidad", "extraer_tasas_activas", "extraer_creditos_directos"):
        monkeypatch.setattr(sbs_extractor, nombre, prohibido)
    datos = versiones.verificar_paquete(paquete, caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
    assert datos["status"] == "complete"
    assert not (caso["store"] / "work").exists()


@pytest.mark.parametrize("operacion", ["ingest", "rebuild"])
def test_contexto_antes_del_procesamiento_y_latest_intacto(caso, monkeypatch, operacion):
    identidad, _ = latest_controlado(caso)
    latest = caso["store"] / "LATEST"
    antes = latest.read_bytes(), latest.stat().st_ino, latest.stat().st_mtime_ns
    bloqueo = caso["store"] / ".publish.lock"
    bloqueo.write_bytes(b"contenido estable")
    inode = bloqueo.stat().st_ino
    original = sbs_extractor.extraer_morosidad if operacion == "ingest" else h.construir_dataset_analitico
    llamadas = []

    def comprobar(*args, **opciones):
        ruta = args[0] if operacion == "ingest" else opciones["ruta_sbs"]
        candidato = next(padre for padre in ruta.parents if padre.parent == caso["store"] / "work")
        contexto = cargar(candidato / CONTEXTO)
        assert contexto == {"schema_version": 1, "base": identidad}
        llamadas.append(contexto)
        return original(*args, **opciones)

    if operacion == "ingest":
        monkeypatch.setattr(sbs_extractor, "extraer_morosidad", comprobar)
        candidato = ingerir(caso)
    else:
        monkeypatch.setattr(h, "construir_dataset_analitico", comprobar)
        candidato = reconstruir(caso)
    assert len(llamadas) == 1
    assert cargar(candidato / "manifest.json")["publication_base"] == identidad
    assert (latest.read_bytes(), latest.stat().st_ino, latest.stat().st_mtime_ns) == antes
    assert bloqueo.stat().st_ino == inode and bloqueo.read_bytes() == b"contenido estable"


def test_herencia_exacta_sin_recapturar_latest(caso, monkeypatch):
    identidad, _ = latest_controlado(caso)
    origen = ingerir(caso)
    contexto = (origen / CONTEXTO).read_bytes()
    (caso["store"] / "LATEST").write_bytes(b"ahora invalido")

    def prohibido(*args, **opciones):
        pytest.fail("No debe recapturar la base al reconstruir desde ingesta")

    monkeypatch.setattr(h, "capturar_contexto", prohibido)
    nuevo = reconstruir(caso, origen.name)
    assert (nuevo / CONTEXTO).read_bytes() == contexto
    assert cargar(nuevo / "manifest.json")["publication_base"] == identidad
    paquete = empaquetar(caso, nuevo)
    assert versiones.verificar_paquete(paquete, caso["raiz"] / h.MANIFIESTO_REFERENCIAS)["publication_base"] == identidad
    assert (caso["store"] / "LATEST").read_bytes() == b"ahora invalido"


@pytest.mark.parametrize("defecto", ["json", "duplicado", "null", "id", "hash", "roto", "enlace", "enlace_roto", "directorio", "paquete_enlace", "versions_enlace", "paquete_datos"])
def test_latest_invalido_no_equivale_a_ausencia(caso, defecto):
    identidad, version = latest_controlado(caso)
    latest = caso["store"] / "LATEST"
    if defecto == "json":
        latest.write_bytes(b"{")
    elif defecto == "duplicado":
        latest.write_bytes(b'{"version_id":"a","version_id":"b","manifest_sha256":"x"}')
    elif defecto == "null":
        latest.write_bytes(b"null")
    elif defecto == "id":
        guardar(latest, {**identidad, "version_id": "../escapar"})
    elif defecto == "hash":
        guardar(latest, {**identidad, "manifest_sha256": "0" * 64})
    elif defecto == "roto":
        (version / "manifest.json").unlink()
    elif defecto in ("enlace", "enlace_roto"):
        latest.unlink()
        latest.symlink_to(version / ("manifest.json" if defecto == "enlace" else "no-existe"))
    elif defecto == "directorio":
        latest.unlink()
        latest.mkdir()
    elif defecto == "paquete_enlace":
        movido = caso["tmp"] / "version-movida"
        version.rename(movido)
        version.symlink_to(movido, target_is_directory=True)
    elif defecto == "paquete_datos":
        (version / "payload" / h.FEATURES_RECONSTRUIDAS).write_bytes(b"datos alterados")
    else:
        original = caso["store"] / "versions"
        movido = caso["tmp"] / "versions-movido"
        original.rename(movido)
        original.symlink_to(movido, target_is_directory=True)
    antes = inventario(caso["store"] / "work")
    with pytest.raises((ValueError, OSError)):
        reconstruir(caso)
    assert inventario(caso["store"] / "work") == antes


@pytest.mark.parametrize("tipo", ["enlace", "enlace_roto", "directorio"])
def test_bloqueo_invalido_no_se_reemplaza(caso, tipo):
    ruta = caso["store"] / ".publish.lock"
    if tipo == "directorio":
        ruta.mkdir()
    else:
        destino = caso["tmp"] / "destino-lock"
        if tipo == "enlace":
            destino.write_bytes(b"intacto")
        ruta.symlink_to(destino)
    inode = ruta.lstat().st_ino
    with pytest.raises((ValueError, OSError)):
        reconstruir(caso)
    assert ruta.lstat().st_ino == inode


def test_captura_utiliza_flock_compartido(caso, monkeypatch):
    flock = fcntl.flock
    operaciones = []

    def observar(fd, operacion):
        operaciones.append(operacion)
        return flock(fd, operacion)

    monkeypatch.setattr(fcntl, "flock", observar)
    datos = json.loads(capturar_contexto(caso["store"], caso["raiz"] / h.MANIFIESTO_REFERENCIAS))
    assert datos["base"] is None
    assert operaciones == [fcntl.LOCK_SH]


def convertir_a_esquema_uno(candidato):
    datos = cargar(candidato / "manifest.json")
    datos["schema_version"] = 1
    datos.pop("publication_base")
    datos.pop("publication_context")
    datos["artifacts"].pop(CONTEXTO)
    (candidato / CONTEXTO).unlink()
    guardar(candidato / "manifest.json", datos)


@pytest.mark.parametrize("origen", ["initial", "ingest"])
def test_esquema_uno_verificable_pero_no_empaquetable(caso, monkeypatch, origen):
    if origen == "initial":
        candidato = reconstruir(caso)
        convertir_a_esquema_uno(candidato)
    else:
        antiguo = ingerir(caso)
        convertir_a_esquema_uno(antiguo)
        h.verificar(caso["store"], caso["raiz"], candidato=antiguo.name)

        def prohibido(*args, **opciones):
            pytest.fail("No debe asignar base retrospectiva")

        monkeypatch.setattr(h, "capturar_contexto", prohibido)
        candidato = reconstruir(caso, antiguo.name)
    datos = h.verificar(caso["store"], caso["raiz"], candidato=candidato.name)
    assert datos["schema_version"] == 1 and "publication_base" not in datos
    assert len(datos["artifacts"]) == (82 if origen == "initial" else 88)
    assert not (candidato / CONTEXTO).exists()
    with pytest.raises(ValueError, match="retrospectivamente"):
        empaquetar(caso, candidato)
    assert not (caso["tmp"] / "paquete").exists()
    assert campos_contexto(candidato, 1) == {}


@pytest.mark.parametrize("alteracion", ["datos", "excel", "informe", "auditoria", "procedencia", "initial", "rebuild", "genealogia", "referencias", "contexto", "extra", "escape", "enlace"])
def test_paquete_alterado_rechazado_incluso_actualizando_inventario(caso, alteracion):
    candidato = reconstruir(caso, ingerir(caso).name)
    paquete = empaquetar(caso, candidato)
    rutas = {"datos": f"payload/{h.RECOMPUESTOS[h.SBS]}", "excel": "payload/inputs/sbs/B-2362-2023-01.xls",
             "informe": f"payload/{ingesta.INFORME}", "auditoria": f"payload/{h.AUDITORIA_P75}",
             "procedencia": f"payload/{ingesta.PROCEDENCIA}", "initial": versiones.MAN_INITIAL,
             "rebuild": versiones.MAN_REBUILD, "genealogia": versiones.MAN_INGEST,
             "referencias": versiones.MAN_REFERENCIAS, "contexto": f"payload/{CONTEXTO}"}
    if alteracion == "extra":
        (paquete / "evidence/extra.txt").write_text("inesperado", encoding="utf-8")
    elif alteracion == "escape":
        datos = cargar(paquete / "manifest.json")
        datos["artifacts"]["../fuera.txt"] = "0" * 64
        guardar(paquete / "manifest.json", datos)
    elif alteracion == "enlace":
        ruta = paquete / "payload" / h.RECOMPUESTOS[h.SBS]
        ruta.unlink()
        ruta.symlink_to(caso["raiz"] / h.SBS)
    else:
        ruta = paquete / rutas[alteracion]
        if alteracion in ("datos", "excel"):
            ruta.write_bytes(b"alterado\n")
        else:
            datos = cargar(ruta)
            if alteracion == "genealogia":
                datos["source_candidate"]["id"] = candidato.name
            elif alteracion == "contexto":
                datos["base"] = {"version_id": "v-" + "0" * 64, "manifest_sha256": "1" * 64}
            else:
                datos["campo_no_permitido"] = True
            guardar(ruta, datos)
        datos = cargar(paquete / "manifest.json")
        datos["artifacts"][rutas[alteracion]] = hashlib.sha256(ruta.read_bytes()).hexdigest()
        guardar(paquete / "manifest.json", datos)
    with pytest.raises((ValueError, OSError)):
        versiones.verificar_paquete(paquete, caso["raiz"] / h.MANIFIESTO_REFERENCIAS)


@pytest.mark.parametrize("ruta", ["../fuera", "/tmp/fuera", "evidence/../../fuera", "payload/./archivo", "payload\\fuera"])
def test_rutas_no_confinadas(caso, ruta):
    with pytest.raises(ValueError):
        versiones.archivo_paquete(caso["tmp"], ruta)


@pytest.mark.parametrize("destino", ["work", "versions", "initial"])
def test_destino_de_paquete_separado(caso, destino):
    candidato = reconstruir(caso)
    rutas = {"work": caso["store"] / "work/nuevo", "versions": caso["store"] / "versions/nuevo", "initial": caso["initial"]}
    antes = inventario(caso["store"])
    with pytest.raises(ValueError, match="separado"):
        versiones._preparar_paquete(caso["store"], candidato.name, rutas[destino], caso["raiz"])
    assert inventario(caso["store"]) == antes


def test_comandos_nuevos_exigen_argumentos_y_no_hay_resume_independiente():
    for comando in ("publish", "resolve", "resume"):
        with pytest.raises(SystemExit) as salida:
            h.main([comando])
        assert salida.value.code == 2


def test_contexto_ausente_no_es_base_null(caso):
    candidato = reconstruir(caso)
    assert cargar(candidato / "manifest.json")["publication_base"] is None
    (candidato / CONTEXTO).unlink()
    with pytest.raises(ValueError, match="contexto"):
        h.verificar(caso["store"], caso["raiz"], candidato=candidato.name)
    with pytest.raises(ValueError, match="contexto"):
        empaquetar(caso, candidato)


def test_fallo_copia_no_declara_paquete_completo(caso, monkeypatch):
    candidato = reconstruir(caso)
    antes = inventario(caso["store"])
    abrir = versiones.archivo_paquete

    def fallar(raiz, ruta):
        if raiz == candidato and ruta == h.RECOMPUESTOS[h.SBS]:
            raise OSError("fallo de copia controlado")
        return abrir(raiz, ruta)

    monkeypatch.setattr(versiones, "archivo_paquete", fallar)
    with pytest.raises(OSError, match="fallo de copia"):
        empaquetar(caso, candidato)
    assert inventario(caso["store"]) == antes
    assert not (caso["tmp"] / "paquete/manifest.json").exists()
    with pytest.raises(OSError):
        versiones.verificar_paquete(caso["tmp"] / "paquete", caso["raiz"] / h.MANIFIESTO_REFERENCIAS)
