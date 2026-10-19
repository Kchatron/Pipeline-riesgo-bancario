"""Ingesta mensual sin red: límites decimales y extractores SBS reales aislados."""

import csv
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import shutil

import pandas as pd
import pytest

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h, ingesta
from src.pipeline.validacion import COLUMNAS_HECHOS, MESES


REPO = Path(__file__).resolve().parents[1]
MES = "2023-01"
FUNCIONES = ("extraer_morosidad", "extraer_creditos_directos", "extraer_tasas_activas")
INDICADORES = ("Morosidad", "Créditos Directos Totales", "Tasa Activa MN")


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
            for ruta in raiz.rglob("*") if ruta.is_file()}


def leer(ruta):
    with ruta.open(encoding="utf-8", newline="") as archivo:
        return list(csv.DictReader(archivo))


def json_leer(ruta):
    return json.loads(ruta.read_bytes())


def csv_escribir(ruta, columnas, filas):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", encoding="utf-8", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=columnas, lineterminator="\n")
        escritor.writeheader()
        escritor.writerows(filas)


def json_escribir(ruta, datos):
    ruta.write_text(json.dumps(datos, ensure_ascii=False), encoding="utf-8")


@pytest.fixture(autouse=True)
def referencias_y_excel_intactos():
    rutas = [REPO / ruta for ruta in h.REFERENCIAS] + list((REPO / "data/raw/SBS").iterdir())
    antes = {ruta: hashlib.sha256(ruta.read_bytes()).hexdigest() for ruta in rutas if ruta.is_file()}
    yield
    assert {ruta: hashlib.sha256(ruta.read_bytes()).hexdigest() for ruta in antes} == antes


@pytest.fixture
def entorno(tmp_path, monkeypatch):
    store = tmp_path / "store"
    origen = h.inicializar(store)
    antes = inventario(origen)
    carpeta = tmp_path / "excel"
    carpeta.mkdir()
    archivos = ("B-2362-2023-01.xls", "B-2334-2023-01.xls", "Tasas-2023-01.xlsx")
    for nombre in archivos:
        (carpeta / nombre).write_bytes(b"Excel controlado: solo se procesa con funciones simuladas")
    sbs = [fila for fila in leer(REPO / h.SBS) if fila["fecha_id"] == MES]
    macro = {fila["indicador_id"]: fila["valor"] for fila in leer(REPO / h.BCRP) if fila["fecha_id"] == MES}
    estado = {"store": store, "origen": origen, "carpeta": carpeta, "sbs": sbs,
              "payload": {"config": {"series": [{"name": codigo} for codigo in bcrp_extractor.SERIES_BCRP]},
                          "periods": [{"name": "Ene.2023", "values": [macro[nombre] for nombre in bcrp_extractor.SERIES_BCRP.values()]}]},
              "solicitudes": [], "sbs_llamadas": [], "mutar_sbs": lambda filas: filas,
              "reales": {nombre: getattr(sbs_extractor, nombre) for nombre in FUNCIONES}}

    def simulador(indicador):
        def extraer(ruta, mes):
            assert ruta.is_relative_to(store / "work") and not ruta.is_relative_to(origen)
            assert ruta.read_bytes() == (carpeta / ruta.name).read_bytes()
            estado["sbs_llamadas"].append((ruta, mes, indicador))
            filas = [fila.copy() for fila in sbs if fila["indicador_id"] == indicador]
            filas = estado["mutar_sbs"](filas)
            return pd.DataFrame(filas, columns=COLUMNAS_HECHOS)
        return extraer

    for nombre, indicador in zip(FUNCIONES, INDICADORES):
        monkeypatch.setattr(sbs_extractor, nombre, simulador(indicador))

    def urlopen(request):
        estado["solicitudes"].append(request.full_url)
        return io.BytesIO(json.dumps(estado["payload"]).encode("utf-8"))

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", urlopen)
    yield estado
    assert inventario(origen) == antes


def ejecutar(entorno):
    return ingesta.ingerir(entorno["store"], "initial", MES, entorno["carpeta"])


def comprobar_bloqueado(entorno, estado="blocked"):
    candidatos = [ruta for ruta in (entorno["store"] / "work").iterdir() if ruta.name != "initial"]
    assert len(candidatos) == 1
    nuevo = candidatos[0]
    assert not (nuevo / "manifest.json").exists()
    informe = json_leer(nuevo / ingesta.INFORME)
    assert informe["status"] == estado
    with pytest.raises(ValueError, match="incompleto"):
        h.verificar(entorno["store"], candidato=nuevo.name)
    return nuevo, informe


def comprobar_exito(entorno, nuevo):
    manifiesto = h.verificar(entorno["store"], candidato=nuevo.name)
    assert manifiesto["stage"] == "historical-ingest"
    assert manifiesto["status"] == "sources-validated"
    assert manifiesto["source_candidate"] == {"id": "initial", "manifest_sha256": hashlib.sha256(
        (entorno["origen"] / "manifest.json").read_bytes()).hexdigest()}
    assert manifiesto["artifacts"] == {ruta: huella for ruta, huella in inventario(nuevo).items() if ruta != "manifest.json"}
    assert len(manifiesto["artifacts"]) == 87
    assert manifiesto["schema_version"] == 2
    assert manifiesto["publication_base"] is None
    assert not (nuevo / h.FEATURES_RECONSTRUIDAS).exists()
    assert not (nuevo / h.AUDITORIA_P75).exists()
    assert not (nuevo / "references" / h.FEATURES).exists()
    assert not (nuevo / "references" / h.DECISIONES).exists()
    assert not (entorno["store"] / "LATEST").exists()
    assert len(leer(nuevo / h.RECOMPUESTOS[h.SBS])) == 1296
    assert len(leer(nuevo / h.RECOMPUESTOS[h.BCRP])) == 108
    assert len(leer(nuevo / h.RECOMPUESTOS[h.MACRO])) == 36
    for mes in MESES:
        assert len(leer(nuevo / f"partitions/{mes}/sbs.csv")) == 36
        assert len(leer(nuevo / f"partitions/{mes}/bcrp.csv")) == 3
        if mes != MES:
            for fuente in ("sbs", "bcrp"):
                ruta = f"partitions/{mes}/{fuente}.csv"
                assert (nuevo / ruta).read_bytes() == (entorno["origen"] / ruta).read_bytes()
                assert not (nuevo / ruta).samefile(entorno["origen"] / ruta)
    informe = json_leer(nuevo / ingesta.INFORME)
    assert informe["status"] == "matched"
    assert len(informe["comparisons"]) == 81
    assert all(fila["accepted"] for fila in informe["comparisons"])
    for fila in informe["comparisons"]:
        delta = Decimal(fila["candidate"]) - Decimal(fila["reference"])
        assert delta == Decimal(fila["difference"])
        assert fila["tokens_equal"] == (fila["reference"] == fila["candidate"])
        assert fila["numeric_equal"] == (delta == 0)
        assert set(fila["key"]) == {"fecha_id", "banco_id", "tipo_credito_id", "indicador_id"}
    return informe


def test_cli_ingest_y_verify_sin_red(entorno, monkeypatch, capsys):
    assert h.main(["ingest", "--store", str(entorno["store"]), "--candidate", "initial",
                   "--month", MES, "--sbs-dir", str(entorno["carpeta"])]) == 0
    texto = capsys.readouterr().out
    identificador = next(linea.split(": ", 1)[1] for linea in texto.splitlines() if linea.startswith("Fuentes ingeridas"))
    nuevo = entorno["store"] / "work" / identificador
    assert f"Ruta: {nuevo}" in texto
    assert h.main(["verify", "--store", str(entorno["store"]), "--candidate", identificador]) == 0
    assert h.main(["verify", "--store", str(entorno["store"])]) == 0
    assert len(entorno["sbs_llamadas"]) == 3
    assert entorno["solicitudes"] == [
        "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PN01207PM-PD04722MM-PN01273PM/json/2023-01/2023-01"
    ]
    comprobar_exito(entorno, nuevo)


def test_integracion_excel_sbs_reales(entorno, monkeypatch):
    for archivo in entorno["carpeta"].iterdir():
        shutil.copyfile(REPO / "data/raw/SBS" / archivo.name, archivo)
    llamadas = []
    validar = sbs_extractor.validar_integridad_temporal

    def integridad(df, ruta, mes, **opciones):
        assert ruta.is_relative_to(entorno["store"] / "work")
        llamadas.append(ruta)
        return validar(df, ruta, mes, **opciones)

    monkeypatch.setattr(sbs_extractor, "validar_integridad_temporal", integridad)
    for nombre, funcion in entorno["reales"].items():
        monkeypatch.setattr(sbs_extractor, nombre, funcion)
    nuevo = ejecutar(entorno)
    assert len(llamadas) == 3
    comprobar_exito(entorno, nuevo)


@pytest.mark.parametrize("mes", ["2022-12", "2026-01", "2023-1", "2023-00", "2023-13", "", "../initial"])
def test_mes_invalido_sin_efectos(entorno, mes):
    antes = inventario(entorno["store"])
    with pytest.raises(ValueError, match="YYYY-MM"):
        ingesta.ingerir(entorno["store"], "initial", mes, entorno["carpeta"])
    assert inventario(entorno["store"]) == antes
    assert not entorno["solicitudes"] and not entorno["sbs_llamadas"]


@pytest.mark.parametrize("defecto", ["ausente", "ambiguo"])
def test_excel_inexistente_o_ambiguo(entorno, defecto):
    ruta = entorno["carpeta"] / "B-2362-2023-01.xls"
    if defecto == "ausente":
        ruta.unlink()
    else:
        ruta.with_suffix(".XLSX").write_bytes(ruta.read_bytes())
    antes = inventario(entorno["store"])
    with pytest.raises(ValueError, match="exactamente un Excel"):
        ejecutar(entorno)
    assert inventario(entorno["store"]) == antes
    assert not entorno["sbs_llamadas"] and not entorno["solicitudes"]


@pytest.mark.parametrize("defecto", ["otro_mes", "duplicado", "nulo", "infinito", "indicador"])
def test_respuesta_bcrp_invalida(entorno, defecto):
    payload = entorno["payload"]
    if defecto == "otro_mes":
        payload["periods"][0]["name"] = "Feb.2023"
    elif defecto == "duplicado":
        payload["periods"].append(payload["periods"][0].copy())
    elif defecto in ("nulo", "infinito"):
        payload["periods"][0]["values"][0] = "n.d." if defecto == "nulo" else "inf"
    else:
        payload["config"]["series"][0]["name"] = "desconocido"
    with pytest.raises(ValueError, match="Evidencia conservada"):
        ejecutar(entorno)
    comprobar_bloqueado(entorno, "failed")


@pytest.mark.parametrize("defecto", ["duplicado", "nulo", "clave", "columnas"])
def test_sbs_invalido(entorno, defecto, monkeypatch):
    def mutar(filas):
        if defecto == "duplicado":
            return [*filas, filas[0].copy()]
        if defecto == "nulo":
            filas[0]["valor"] = None
        elif defecto == "clave":
            filas[0]["banco_id"] = "Fuera del alcance"
        return filas

    entorno["mutar_sbs"] = mutar
    if defecto == "columnas":
        monkeypatch.setattr(sbs_extractor, FUNCIONES[0], lambda ruta, mes: pd.DataFrame({"otra": [1]}))
    with pytest.raises(ValueError, match="Evidencia conservada"):
        ejecutar(entorno)
    comprobar_bloqueado(entorno, "failed")


def test_sbs_diferencia_numerica_bloquea_y_conserva_informe(entorno):
    def mutar(filas):
        filas[0]["valor"] = str(Decimal(filas[0]["valor"]) + Decimal("0.000000000000000001"))
        return filas

    entorno["mutar_sbs"] = mutar
    with pytest.raises(ValueError, match="decisión humana"):
        ejecutar(entorno)
    nuevo, informe = comprobar_bloqueado(entorno)
    rechazadas = [fila for fila in informe["comparisons"] if not fila["accepted"]]
    assert rechazadas and all(fila["dataset"] == "SBS" for fila in rechazadas)
    assert {fila["against"] for fila in rechazadas} == {"approved-reference", "initial"}
    assert all(Decimal(fila["difference"]) == Decimal("1e-18") for fila in rechazadas)
    assert not (nuevo / h.RECOMPUESTOS[h.SBS]).exists()
    assert (nuevo / ingesta.BCRP_MES).is_file() and (nuevo / ingesta.SBS_MES).is_file()


def test_sbs_tokens_diferentes_numericamente_iguales(entorno):
    def mutar(filas):
        for fila in filas:
            fila["valor"] += "0"
        return filas

    entorno["mutar_sbs"] = mutar
    nuevo = ejecutar(entorno)
    informe = comprobar_exito(entorno, nuevo)
    sbs = [fila for fila in informe["comparisons"] if fila["dataset"] == "SBS"]
    assert all(fila["numeric_equal"] and not fila["tokens_equal"] for fila in sbs)
    assert leer(nuevo / ingesta.SBS_MES)[0]["valor"].endswith("0")


@pytest.mark.parametrize("delta, aceptado", [("0.000000000999999999", True), ("0.000000001", True),
                                            ("0.000000001000000001", False), ("-0.000000001", True)])
def test_limite_bcrp_decimal_sin_redondeos(entorno, monkeypatch, delta, aceptado):
    extractor = bcrp_extractor.extraer_bcrp_api
    base = next(fila for fila in leer(REPO / h.BCRP)
                if fila["fecha_id"] == MES and fila["indicador_id"] == "tipo_cambio")["valor"]
    valor = str(Decimal(base) + Decimal(delta))

    def ajustar_tokens(**opciones):
        resultado = extractor(**opciones)
        largo = opciones["directorio_salida"] / Path(ingesta.BCRP_MES).name
        ancho = opciones["directorio_salida"] / Path(ingesta.MACRO_MES).name
        filas = leer(largo)
        for fila in filas:
            if fila["indicador_id"] == "tipo_cambio":
                fila["valor"] = valor
        csv_escribir(largo, COLUMNAS_HECHOS, filas)
        filas = leer(ancho)
        filas[0]["tipo_cambio"] = valor
        csv_escribir(ancho, ("fecha_id", "tipo_cambio", "tasa_referencia", "inflacion"), filas)
        return resultado

    monkeypatch.setattr(bcrp_extractor, "extraer_bcrp_api", ajustar_tokens)
    if aceptado:
        nuevo = ejecutar(entorno)
        informe = comprobar_exito(entorno, nuevo)
        assert next(fila for fila in leer(nuevo / h.RECOMPUESTOS[h.BCRP])
                    if fila["fecha_id"] == MES and fila["indicador_id"] == "tipo_cambio")["valor"] == valor
    else:
        with pytest.raises(ValueError, match="decisión humana"):
            ejecutar(entorno)
        _, informe = comprobar_bloqueado(entorno)
    diferencias = [fila for fila in informe["comparisons"]
                   if fila["against"] in ("initial", "approved-reference") and fila["dataset"] == "BCRP"
                   and fila["key"]["indicador_id"] == "tipo_cambio"]
    assert len(diferencias) == 2
    assert all(Decimal(fila["difference"]) == Decimal(delta) and fila["accepted"] == aceptado for fila in diferencias)


def test_bcrp_ancho_largo_incoherentes(entorno, monkeypatch):
    extractor = bcrp_extractor.extraer_bcrp_api

    def incoherente(**opciones):
        resultado = extractor(**opciones)
        ruta = opciones["directorio_salida"] / Path(ingesta.MACRO_MES).name
        filas = leer(ruta)
        filas[0]["tipo_cambio"] = str(Decimal(filas[0]["tipo_cambio"]) + Decimal("1e-10"))
        csv_escribir(ruta, ("fecha_id", "tipo_cambio", "tasa_referencia", "inflacion"), filas)
        return resultado

    monkeypatch.setattr(bcrp_extractor, "extraer_bcrp_api", incoherente)
    with pytest.raises(ValueError, match="decisión humana"):
        ejecutar(entorno)
    _, informe = comprobar_bloqueado(entorno)
    assert any(not fila["accepted"] and fila["against"] == "wide/long" for fila in informe["comparisons"])


def test_repeticion_equivalente_y_aislada(entorno):
    primero = ejecutar(entorno)
    antes = inventario(primero)
    segundo = ejecutar(entorno)
    assert primero != segundo and inventario(primero) == antes
    for nuevo in (primero, segundo):
        comprobar_exito(entorno, nuevo)
    for ruta in (ingesta.INFORME, ingesta.PROCEDENCIA, *h.RECOMPUESTOS.values()):
        assert (primero / ruta).read_bytes() == (segundo / ruta).read_bytes()


@pytest.mark.parametrize("fuente", ["SBS", "BCRP"])
def test_fallos_extractores_conservan_evidencia(entorno, monkeypatch, fuente):
    def fallar(*args, **opciones):
        raise RuntimeError("fallo controlado de extracción")

    if fuente == "SBS":
        monkeypatch.setattr(sbs_extractor, FUNCIONES[0], fallar)
    else:
        monkeypatch.setattr(bcrp_extractor, "extraer_bcrp_api", fallar)
    with pytest.raises(ValueError, match="fallo controlado"):
        ejecutar(entorno)
    nuevo, informe = comprobar_bloqueado(entorno, "failed")
    assert "fallo controlado" in informe["error"]
    assert (nuevo / ingesta.PROCEDENCIA).is_file()
    assert len(list((nuevo / "inputs/sbs").iterdir())) == 3


@pytest.mark.parametrize("candidato", ["otro", "../initial", "rebuild-cualquiera"])
def test_origen_solo_initial(entorno, candidato):
    antes = inventario(entorno["store"])
    with pytest.raises(ValueError, match="únicamente"):
        ingesta.ingerir(entorno["store"], candidato, MES, entorno["carpeta"])
    assert inventario(entorno["store"]) == antes


@pytest.mark.parametrize("artefacto", ["informe", "excel", "otra_particion", "consolidado", "inventario", "origen", "procedencia", "bcrp_ausente"])
def test_verify_detecta_alteraciones_sin_red(entorno, monkeypatch, artefacto):
    nuevo = ejecutar(entorno)
    manifiesto = json_leer(nuevo / "manifest.json")
    if artefacto == "informe":
        ruta = nuevo / ingesta.INFORME
        informe = json_leer(ruta)
        informe["comparisons"][0]["difference"] = "999"
        json_escribir(ruta, informe)
    elif artefacto == "excel":
        ruta = next((nuevo / "inputs/sbs").iterdir())
        ruta.write_bytes(b"otro Excel")
    elif artefacto == "otra_particion":
        ruta = nuevo / "partitions/2023-02/sbs.csv"
        ruta.write_bytes(ruta.read_bytes() + b"\n")
    elif artefacto == "consolidado":
        ruta = nuevo / h.RECOMPUESTOS[h.SBS]
        filas = leer(ruta)
        filas[0]["valor"] = str(Decimal(filas[0]["valor"]) + 1)
        csv_escribir(ruta, COLUMNAS_HECHOS, filas)
    elif artefacto == "inventario":
        ruta = nuevo / "extra.txt"
        ruta.write_text("no esperado", encoding="utf-8")
    elif artefacto == "origen":
        manifiesto["source_candidate"]["manifest_sha256"] = "0" * 64
        ruta = None
    elif artefacto == "procedencia":
        ruta = nuevo / ingesta.PROCEDENCIA
        entradas = json_leer(ruta)
        entradas["bcrp"]["end"] = "2025-12"
        json_escribir(ruta, entradas)
        manifiesto["inputs"] = entradas
    else:
        ruta = nuevo / ingesta.BCRP_MES
        ruta.unlink()
    if ruta and ruta.is_file():
        manifiesto["artifacts"][ruta.relative_to(nuevo).as_posix()] = hashlib.sha256(ruta.read_bytes()).hexdigest()
    json_escribir(nuevo / "manifest.json", manifiesto)

    def prohibido(*args, **opciones):
        pytest.fail("verify no puede ejecutar extractores ni consultar la red")

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", prohibido)
    for nombre in FUNCIONES:
        monkeypatch.setattr(sbs_extractor, nombre, prohibido)
    with pytest.raises((ValueError, OSError)):
        h.verificar(entorno["store"], candidato=nuevo.name)


def test_reconstruye_desde_ingest(entorno):
    ingerido = ejecutar(entorno)
    antes = inventario(ingerido)
    nuevo = h.reconstruir(entorno["store"], candidato=ingerido.name)
    manifiesto = h.verificar(entorno["store"], candidato=nuevo.name)
    assert manifiesto["source_candidate"]["id"] == ingerido.name
    assert manifiesto["stage"] == "historical-rebuild"
    assert inventario(ingerido) == antes


def test_initial_incompleto_se_rechaza_antes_de_extraer(entorno):
    ruta = entorno["origen"] / "partitions/2025-12/bcrp.csv"
    contenido = ruta.read_bytes()
    ruta.unlink()
    antes = inventario(entorno["store"])
    try:
        with pytest.raises(ValueError, match="incompleto"):
            ejecutar(entorno)
        assert inventario(entorno["store"]) == antes
        assert not entorno["solicitudes"] and not entorno["sbs_llamadas"]
    finally:
        ruta.write_bytes(contenido)


def test_cli_discrepancia_devuelve_error_y_ruta_de_evidencia(entorno, capsys):
    def mutar(filas):
        filas[0]["valor"] = str(Decimal(filas[0]["valor"]) + 1)
        return filas

    entorno["mutar_sbs"] = mutar
    assert h.main(["ingest", "--store", str(entorno["store"]), "--candidate", "initial",
                   "--month", MES, "--sbs-dir", str(entorno["carpeta"])]) == 1
    nuevo, _ = comprobar_bloqueado(entorno)
    assert str(nuevo) in capsys.readouterr().err
