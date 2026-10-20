"""Contrato CLI JSON: procesos reales, API simulada y fallos con evidencia."""

import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h, publicacion as p


REPO = Path(__file__).resolve().parents[1]


def huella(ruta):
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): huella(ruta) for ruta in raiz.rglob("*") if ruta.is_file()}


def documento(texto, operacion):
    resultado = json.loads(texto)  # Rechaza progreso o documentos adicionales en stdout.
    assert resultado["schema_version"] == 1
    assert resultado["operation"] == operacion
    return resultado


def comprobar_manifiesto(resultado, ruta, version=False):
    manifiesto = json.loads((ruta / "manifest.json").read_bytes())
    assert resultado["version_id" if version else "candidate_id"] == ruta.name
    assert resultado["path"] == str(ruta.resolve())
    assert resultado["manifest_sha256"] == huella(ruta / "manifest.json")
    assert resultado["stage"] == manifiesto["stage"]
    assert resultado["status"] == manifiesto["status"]
    claves = {"schema_version", "operation", "path", "manifest_sha256", "stage", "status",
              "version_id" if version else "candidate_id"}
    for campo in ("month", "source_candidate"):
        if campo in manifiesto:
            claves.add(campo)
            assert resultado[campo] == manifiesto[campo]
    assert set(resultado) == claves


@pytest.fixture(autouse=True)
def sin_red_y_referencias_intactas(monkeypatch):
    antes = {ruta: huella(REPO / ruta) for ruta in h.REFERENCIAS}

    def prohibido(*args, **opciones):
        pytest.fail("No se permite consultar la API real")

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", prohibido)
    yield
    assert {ruta: huella(REPO / ruta) for ruta in h.REFERENCIAS} == antes


@pytest.fixture
def inicial(tmp_path):
    store = tmp_path / "store"
    initial = h.inicializar(store)
    antes = inventario(initial)
    yield store
    assert inventario(initial) == antes


def cli(store, capsys, *argumentos, salida_json=True):
    capsys.readouterr()
    codigo = h.main([argumentos[0], "--store", str(store), *argumentos[1:], *(["--json"] if salida_json else [])])
    salida = capsys.readouterr()
    return codigo, salida.out, salida.err


@pytest.mark.parametrize("salida_json", [False, True])
def test_cli_proceso_completo_json_y_compatibilidad(tmp_path, salida_json):
    store = tmp_path / "store"

    def ejecutar(*argumentos):
        resultado = subprocess.run(
            [sys.executable, "-B", "-m", "src.pipeline.historico", argumentos[0], "--store", str(store),
             *argumentos[1:], *(["--json"] if salida_json else [])],
            cwd=REPO, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, text=True, capture_output=True,
        )
        assert resultado.returncode == 0, resultado.stderr
        return resultado.stdout, resultado.stderr

    texto, error = ejecutar("init")
    initial = store / "work/initial"
    if salida_json:
        comprobar_manifiesto(documento(texto, "init"), initial)
    else:
        assert texto == f"Candidato preparado: {initial}\n"
    assert error == ""
    texto, error = ejecutar("verify")
    if salida_json:
        comprobar_manifiesto(documento(texto, "verify"), initial)
    else:
        assert texto == "Candidato verificado: 36 particiones, referencias y recomposición correctas.\n"
    assert error == ""
    texto, error = ejecutar("rebuild", "--candidate", "initial")
    reconstruido = next(ruta for ruta in (store / "work").iterdir() if ruta.name != "initial")
    if salida_json:
        comprobar_manifiesto(documento(texto, "rebuild"), reconstruido)
        assert "Dataset ML guardado" in error
    else:
        assert "Dataset ML guardado" in texto
        assert texto.endswith(f"Candidato reconstruido: {reconstruido.name}\nRuta: {reconstruido}\n")
        assert error == ""
    texto, error = ejecutar("verify", "--candidate", reconstruido.name)
    if salida_json:
        comprobar_manifiesto(documento(texto, "verify"), reconstruido)
    else:
        assert texto == "Candidato verificado: 36 particiones, referencias y recomposición correctas.\n"
    assert error == ""

    texto, error = ejecutar("publish", "--candidate", reconstruido.name)
    publicado = documento(texto, "publish") if salida_json else json.loads(texto)
    assert publicado["already_current"] is False and error == ""
    if salida_json:
        assert publicado["status"] == "published"
        assert set(publicado) == {"schema_version", "operation", "status", "version_id", "manifest_sha256", "path", "already_current"}
    else:
        assert set(publicado) == {"version_id", "manifest_sha256", "path", "already_current"}
    version = Path(publicado["path"])
    assert publicado["manifest_sha256"] == huella(version / "manifest.json")
    texto, error = ejecutar("verify", "--version", publicado["version_id"])
    if salida_json:
        comprobar_manifiesto(documento(texto, "verify"), version, version=True)
    else:
        assert texto == f"Versión verificada: {publicado['version_id']}\n"
    assert error == ""
    texto, error = ejecutar("resolve")
    resuelto = documento(texto, "resolve") if salida_json else json.loads(texto)
    esperado = p.resolver_latest(store).como_dict()
    assert resuelto == ({"schema_version": 1, "operation": "resolve", "status": "resolved", **esperado}
                        if salida_json else esperado)
    assert error == ""
    antes = inventario(store)
    texto, error = ejecutar("publish", "--resume-version", publicado["version_id"])
    repetido = documento(texto, "publish") if salida_json else json.loads(texto)
    assert repetido["already_current"] is True and error == ""
    assert repetido["version_id"] == publicado["version_id"]
    assert repetido["manifest_sha256"] == publicado["manifest_sha256"]
    if salida_json:
        assert repetido["status"] == "already-current"
    assert inventario(store) == antes


@pytest.fixture
def fuentes(inicial, tmp_path, monkeypatch):
    carpeta = tmp_path / "excel"
    carpeta.mkdir()
    for nombre in ("B-2362-2023-01.xls", "B-2334-2023-01.xls", "Tasas-2023-01.xlsx"):
        (carpeta / nombre).write_bytes(b"entrada controlada")
    sbs = pd.read_csv(REPO / h.SBS, dtype=str)

    def extraer(indicador):
        return lambda ruta, mes: sbs[sbs.fecha_id.eq(mes) & sbs.indicador_id.eq(indicador)].copy()

    for nombre, indicador in (("extraer_morosidad", "Morosidad"), ("extraer_creditos_directos", "Créditos Directos Totales"),
                              ("extraer_tasas_activas", "Tasa Activa MN")):
        monkeypatch.setattr(sbs_extractor, nombre, extraer(indicador))
    with (REPO / h.BCRP).open(newline="", encoding="utf-8") as archivo:
        valores = {fila["indicador_id"]: fila["valor"] for fila in csv.DictReader(archivo) if fila["fecha_id"] == "2023-01"}
    payload = {"config": {"series": [{"name": codigo} for codigo in bcrp_extractor.SERIES_BCRP]},
               "periods": [{"name": "Ene.2023", "values": [valores[nombre] for nombre in bcrp_extractor.SERIES_BCRP.values()]}]}
    solicitudes = []

    def responder(request):
        solicitudes.append(request.full_url)
        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", responder)
    return carpeta, payload, solicitudes


@pytest.mark.parametrize("salida_json", [False, True])
def test_ingest_y_verify_json_con_extractor_bcrp_real(inicial, fuentes, capsys, salida_json):
    carpeta, _, solicitudes = fuentes
    codigo, texto, error = cli(inicial, capsys, "ingest", "--candidate", "initial", "--month", "2023-01",
                               "--sbs-dir", str(carpeta), salida_json=salida_json)
    assert codigo == 0
    candidato = next(ruta for ruta in (inicial / "work").iterdir() if ruta.name != "initial")
    if salida_json:
        resultado = documento(texto, "ingest")
        comprobar_manifiesto(resultado, candidato)
        assert resultado["month"] == "2023-01" and resultado["status"] == "sources-validated"
        assert "BCRP:" in error and "BCRP (Esquema estrella)" in error
    else:
        assert "BCRP:" in texto
        assert texto.endswith(f"Fuentes ingeridas y validadas: {candidato.name}\nRuta: {candidato}\n")
        assert error == ""
    assert len(solicitudes) == 1
    codigo, texto, error = cli(inicial, capsys, "verify", "--candidate", candidato.name)
    assert codigo == 0 and error == ""
    comprobar_manifiesto(documento(texto, "verify"), candidato)
    assert len(solicitudes) == 1


@pytest.mark.parametrize("salida_json", [False, True])
def test_ingest_fallido_conserva_evidencia_y_codigo_uno(inicial, fuentes, capsys, salida_json):
    carpeta, payload, _ = fuentes
    payload["periods"][0]["name"] = "Feb.2023"
    codigo, texto, error = cli(inicial, capsys, "ingest", "--candidate", "initial", "--month", "2023-01",
                               "--sbs-dir", str(carpeta), salida_json=salida_json)
    assert codigo == 1
    assert "Evidencia conservada" in error
    if salida_json:
        assert texto == "" and "BCRP:" in error
    else:
        assert "BCRP:" in texto and "Fuentes ingeridas y validadas" not in texto
    candidato = next(ruta for ruta in (inicial / "work").iterdir() if ruta.name != "initial")
    assert str(candidato) in error
    assert json.loads((candidato / "audit/differences.json").read_text())["status"] == "failed"
    assert not (candidato / "manifest.json").exists()


def test_error_observador_no_emite_exito_y_conserva_diagnostico(inicial, monkeypatch, capsys):
    def fallar(datos, sbs):
        print("observador iniciado")
        raise ValueError("observador fallido")

    monkeypatch.setattr(h, "auditar_p75", fallar)
    codigo, texto, error = cli(inicial, capsys, "rebuild", "--candidate", "initial")
    assert codigo == 1 and texto == ""
    assert "observador iniciado" in error and "observador fallido" in error
    nuevo = next(ruta for ruta in (inicial / "work").iterdir() if ruta.name != "initial")
    diagnostico = nuevo / h.DIAGNOSTICO_REBUILD
    assert str(diagnostico) in error
    assert json.loads(diagnostico.read_bytes())["status"] == "failed"
    assert not (nuevo / "manifest.json").exists()


@pytest.mark.parametrize("argumentos", [
    ["init", "--json"], ["ingest", "--store", "x", "--json"],
    ["publish", "--store", "x", "--json", "--candidate", "a", "--resume-version", "b"],
    ["verify", "--store", "x", "--json", "--candidate", "a", "--version", "b"],
])
def test_argumentos_invalidos_conservan_codigo_dos_sin_json(argumentos, capsys):
    with pytest.raises(SystemExit) as error:
        h.main(argumentos)
    assert error.value.code == 2
    salida = capsys.readouterr()
    assert salida.out == "" and "error:" in salida.err


@pytest.mark.parametrize("salida_json", [False, True])
def test_base_obsoleta_conserva_codigo_tres_y_latest(inicial, capsys, salida_json):
    a, b = h.reconstruir(inicial), h.reconstruir(inicial)
    p.publicar(inicial, candidato=a.name)
    antes = (inicial / "LATEST").read_bytes()
    codigo, texto, error = cli(inicial, capsys, "publish", "--candidate", b.name, salida_json=salida_json)
    assert codigo == 3 and texto == "" and "Base obsoleta" in error
    assert (inicial / "LATEST").read_bytes() == antes
    diagnostico = next(inicial.glob(".publication-error-*.json"))
    assert json.loads(diagnostico.read_bytes())["exit_code"] == 3


@pytest.mark.parametrize("salida_json", [False, True])
@pytest.mark.parametrize("fallo,esperado", [("antes", 1), ("fsync-despues", 4), ("confirmacion", 4)])
def test_publicacion_incierta_no_emite_exito_y_permite_reanudar(inicial, capsys, monkeypatch, salida_json, fallo, esperado):
    origen = h.reconstruir(inicial)
    sincronizar, reemplazar = p._sincronizar_directorio, p.os.replace

    def sync(ruta):
        if Path(ruta) == inicial and (fallo == "antes" or (fallo == "fsync-despues" and (inicial / "LATEST").exists())):
            raise OSError("sincronización controlada")
        return sincronizar(ruta)

    def replace(origen, destino):
        reemplazar(origen, destino)
        if Path(destino) == inicial / "LATEST":
            raise OSError("confirmación perdida")

    with monkeypatch.context() as cambio:
        cambio.setattr(p, "_sincronizar_directorio", sync)
        if fallo == "confirmacion":
            cambio.setattr(p.os, "replace", replace)
        codigo, texto, error = cli(inicial, capsys, "publish", "--candidate", origen.name, salida_json=salida_json)
    assert codigo == esperado and texto == ""
    assert "Diagnóstico:" in error
    diagnostico = next(inicial.glob(".publication-error-*.json"))
    assert json.loads(diagnostico.read_bytes())["exit_code"] == esperado
    assert (inicial / "LATEST").exists() == (esperado == 4)
    if esperado == 4:
        identidad = json.loads((inicial / "LATEST").read_text())
        codigo, texto, error = cli(inicial, capsys, "publish", "--resume-version", identidad["version_id"])
        assert codigo == 0 and error == ""
        resultado = documento(texto, "publish")
        assert resultado["already_current"] is True and resultado["status"] == "already-current"
        assert resultado["version_id"] == identidad["version_id"]
        assert resultado["manifest_sha256"] == identidad["manifest_sha256"]
        assert len(list((inicial / "versions").iterdir())) == 1


def test_resultados_conservan_identidad_aunque_latest_avance(inicial, monkeypatch, capsys):
    origen = h.reconstruir(inicial)
    publicar = p.publicar
    publicados = []

    def publicar_y_avanzar(*args, **opciones):
        resultado = publicar(*args, **opciones)
        siguiente = h.reconstruir(inicial)
        publicados.append(publicar(inicial, candidato=siguiente.name))
        return resultado

    with monkeypatch.context() as cambio:
        cambio.setattr(p, "publicar", publicar_y_avanzar)
        codigo, texto, _ = cli(inicial, capsys, "publish", "--candidate", origen.name)
    a = documento(texto, "publish")
    assert codigo == 0
    assert a["version_id"] != publicados[0]["version_id"]

    def prohibido(*args):
        pytest.fail("verify --version no puede resolver LATEST")

    with monkeypatch.context() as cambio:
        cambio.setattr(p, "resolver_latest", prohibido)
        codigo, texto, error = cli(inicial, capsys, "verify", "--version", a["version_id"])
    assert codigo == 0 and error == ""
    verificado = documento(texto, "verify")
    assert verificado["version_id"] == a["version_id"]
    assert verificado["manifest_sha256"] == a["manifest_sha256"]

    resolver = p.resolver_latest
    capturas = []

    def resolver_y_avanzar(*args):
        resultado = resolver(*args)
        capturas.append(resultado.version_id)
        siguiente = h.reconstruir(inicial)
        publicar(inicial, candidato=siguiente.name)
        return resultado

    monkeypatch.setattr(p, "resolver_latest", resolver_y_avanzar)
    codigo, texto, _ = cli(inicial, capsys, "resolve")
    resuelto = documento(texto, "resolve")
    assert codigo == 0 and capturas == [publicados[0]["version_id"]]
    assert resuelto["version_id"] == publicados[0]["version_id"]
    assert resuelto["manifest_sha256"] == publicados[0]["manifest_sha256"]
    assert resuelto["version_id"] != json.loads((inicial / "LATEST").read_text())["version_id"]
