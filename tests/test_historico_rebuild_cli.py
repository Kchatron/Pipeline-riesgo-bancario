"""Pruebas del CLI con referencias reales y comprobaciones numéricas independientes."""

import hashlib
import json
from itertools import product
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from src.pipeline import historico


REPOSITORIO = Path(__file__).resolve().parents[1]
CLAVES = ["fecha_id", "banco_id", "tipo_credito_id"]
BANCOS = ("BCP", "BBVA", "Interbank", "Scotiabank", "Mibanco")
CARTERAS = ("Consumo", "Hipotecario")
MESES = tuple(f"{anio}-{mes:02d}" for anio in (2023, 2024, 2025) for mes in range(1, 13))


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
            for ruta in raiz.rglob("*") if ruta.is_file()}


def json_archivo(ruta):
    return json.loads(ruta.read_bytes())


def escribir_json(ruta, valor):
    ruta.write_text(json.dumps(valor, ensure_ascii=False, allow_nan=False), encoding="utf-8")


def actualizar_huella(candidato, ruta):
    """Prueba las validaciones semánticas aun si se reescriben los hashes."""
    manifiesto = json_archivo(candidato / "manifest.json")
    huella = hashlib.sha256((candidato / ruta).read_bytes()).hexdigest()
    manifiesto["artifacts"][ruta] = huella
    for resultado in manifiesto["results"].values():
        if resultado["path"] == ruta:
            resultado["sha256"] = huella
    escribir_json(candidato / "manifest.json", manifiesto)


def leer_features(ruta):
    columnas = pd.read_csv(ruta, nrows=0).columns
    tipos = {columna: "float64" for columna in columnas[3:-1]}
    tipos["riesgo_alto"] = "int64"
    return pd.read_csv(ruta, float_precision="round_trip", dtype=tipos)


def comprobar_resultados_independientes(candidato):
    referencia = leer_features(REPOSITORIO / historico.FEATURES)
    resultado = leer_features(candidato / historico.FEATURES_RECONSTRUIDAS)
    assert resultado.shape == (340, 23)
    assert list(resultado.columns) == list(referencia.columns)
    assert not resultado.isna().any().any()
    assert not resultado.duplicated(CLAVES).any()
    assert resultado.riesgo_alto.isin((0, 1)).all()
    assert "Total Banca Múltiple" not in set(resultado.banco_id)
    assert set(resultado[CLAVES].itertuples(index=False, name=None)) == set(product(MESES[2:], BANCOS, CARTERAS))
    pd.testing.assert_frame_equal(
        resultado.set_index(CLAVES).sort_index(), referencia.set_index(CLAVES).sort_index(), check_exact=True
    )
    sbs = pd.read_csv(REPOSITORIO / historico.SBS, float_precision=None)
    train = sbs[sbs.banco_id.isin(BANCOS) & sbs.indicador_id.eq("Morosidad")
                & sbs.fecha_id.between("2023-01", "2024-12")]
    grupos = train.groupby(["banco_id", "tipo_credito_id"])
    cortes = grupos.valor.quantile(.75, interpolation="linear")
    assert grupos.size().eq(24).all() and len(train) == 240
    auditoria = json_archivo(candidato / historico.AUDITORIA_P75)
    assert auditoria["equal"] is True and auditoria["comparison"] == "exact"
    assert auditoria["interpolation"] == "linear" and auditoria["quantile"] == .75
    assert len(auditoria["panels"]) == 10
    for panel in auditoria["panels"]:
        clave = panel["banco_id"], panel["tipo_credito_id"]
        assert panel["observed_cut"] == panel["expected_cut"] == cortes.loc[clave]
        assert panel["count"] == 24 and panel["equal"] is True
        fuente = grupos.get_group(clave).sort_values("fecha_id")
        assert [fila["fecha_id"] for fila in panel["population"]] == list(MESES[:24])
        assert [fila["morosidad_t"] for fila in panel["population"]] == fuente.valor.tolist()
    limites = np.array([cortes.loc[banco, cartera]
                        for banco, cartera in resultado[CLAVES[1:]].itertuples(index=False, name=None)])
    assert resultado.riesgo_alto.eq((resultado.morosidad_t > limites).astype("int64")).all()
    manifiesto = json_archivo(candidato / "manifest.json")
    assert manifiesto["stage"] == "historical-rebuild"
    assert manifiesto["candidate"] == candidato.name
    assert len(manifiesto["artifacts"]) == 83
    assert manifiesto["schema_version"] == 2
    assert manifiesto["publication_base"] is None
    assert "manifest.json" not in manifiesto["artifacts"]
    assert manifiesto["artifacts"] == {ruta: huella for ruta, huella in inventario(candidato).items()
                                       if ruta != "manifest.json"}
    for ruta in (historico.SBS, historico.BCRP):
        entrada = manifiesto["inputs"][ruta]
        assert entrada == {"path": historico.RECOMPUESTOS[ruta],
                           "sha256": inventario(candidato)[historico.RECOMPUESTOS[ruta]]}
    for ruta, huella in manifiesto["references"].items():
        assert huella == hashlib.sha256((REPOSITORIO / ruta).read_bytes()).hexdigest()


@pytest.fixture(autouse=True)
def referencias_intactas():
    antes = {ruta: (REPOSITORIO / ruta).read_bytes() for ruta in historico.REFERENCIAS}
    yield
    assert {ruta: (REPOSITORIO / ruta).read_bytes() for ruta in antes} == antes


@pytest.fixture
def inicial(tmp_path):
    store = tmp_path / "store"
    origen = historico.inicializar(store)
    antes = inventario(origen)
    yield store, origen
    assert inventario(origen) == antes


@pytest.fixture
def reconstruido(inicial):
    store, origen = inicial
    nuevo = historico.reconstruir(store)
    return store, origen, nuevo


def test_cli_init_rebuild_verify(tmp_path):
    store = tmp_path / "cli-store"

    def ejecutar(*argumentos):
        resultado = subprocess.run(
            [sys.executable, "-B", "-m", "src.pipeline.historico", *argumentos, "--store", str(store)],
            cwd=REPOSITORIO, text=True, capture_output=True
        )
        assert resultado.returncode == 0, resultado.stderr
        return resultado.stdout

    ejecutar("init")
    origen = store / "work/initial"
    antes = inventario(origen)
    salida = ejecutar("rebuild", "--candidate", "initial")
    identificador = next(linea.removeprefix("Candidato reconstruido: ")
                         for linea in salida.splitlines() if linea.startswith("Candidato reconstruido: "))
    nuevo = store / "work" / identificador
    assert f"Ruta: {nuevo}" in salida
    ejecutar("verify", "--candidate", identificador)
    ejecutar("verify")
    assert inventario(origen) == antes
    assert nuevo != origen and not nuevo.is_symlink()
    assert not (store / "LATEST").exists() and not (store / "versions").exists()
    comprobar_resultados_independientes(nuevo)
    manifiesto = json_archivo(nuevo / "manifest.json")
    assert manifiesto["source_candidate"] == {
        "id": "initial", "manifest_sha256": hashlib.sha256((origen / "manifest.json").read_bytes()).hexdigest()
    }


@pytest.mark.parametrize("defecto", ["valor", "ausente", "columnas", "calendario", "duplicado", "nulo", "etiqueta", "agregado"])
def test_features_invalidas_se_rechazan(reconstruido, defecto):
    store, _, nuevo = reconstruido
    ruta = nuevo / historico.FEATURES_RECONSTRUIDAS
    if defecto == "ausente":
        ruta.unlink()
    else:
        datos = leer_features(ruta)
        if defecto == "valor":
            datos.loc[0, "morosidad_t"] = np.nextafter(datos.loc[0, "morosidad_t"], np.inf)
        elif defecto == "columnas":
            datos = datos[datos.columns[::-1]]
        elif defecto == "calendario":
            datos.loc[0, "fecha_id"] = "2023-01"
        elif defecto == "duplicado":
            datos.loc[0, CLAVES] = datos.loc[1, CLAVES]
        elif defecto == "nulo":
            datos.loc[0, "morosidad_t"] = np.nan
        elif defecto == "etiqueta":
            datos.loc[0, "riesgo_alto"] = 1 - datos.loc[0, "riesgo_alto"]
        else:
            datos.loc[0, "banco_id"] = "Total Banca Múltiple"
        datos.to_csv(ruta, index=False)
        actualizar_huella(nuevo, historico.FEATURES_RECONSTRUIDAS)
    with pytest.raises(ValueError):
        historico.verificar(store, candidato=nuevo.name)


@pytest.mark.parametrize("defecto", ["corte", "poblacion", "conteo", "comparacion", "ausente"])
def test_auditoria_invalida_se_rechaza(reconstruido, defecto):
    store, _, nuevo = reconstruido
    ruta = nuevo / historico.AUDITORIA_P75
    if defecto == "ausente":
        ruta.unlink()
    else:
        datos = json_archivo(ruta)
        if defecto == "corte":
            datos["panels"][0]["observed_cut"] = np.nextafter(datos["panels"][0]["observed_cut"], np.inf)
        elif defecto == "poblacion":
            datos["panels"][0]["population"][0]["morosidad_t"] += 1
        elif defecto == "conteo":
            datos["panels"][0]["count"] = 22
        else:
            datos["equal"] = False
        escribir_json(ruta, datos)
        actualizar_huella(nuevo, historico.AUDITORIA_P75)
    with pytest.raises(ValueError):
        historico.verificar(store, candidato=nuevo.name)


def test_entrada_incompleta_no_invoca_constructor(inicial, monkeypatch):
    store, origen = inicial
    ruta = origen / historico.RECOMPUESTOS[historico.BCRP]
    contenido = ruta.read_bytes()
    ruta.unlink()
    antes = inventario(store)

    def prohibido(**opciones):
        pytest.fail("No se debe invocar el constructor con una entrada ausente")

    monkeypatch.setattr(historico, "construir_dataset_analitico", prohibido)
    try:
        with pytest.raises(ValueError, match="incompleto"):
            historico.reconstruir(store)
        assert inventario(store) == antes
    finally:
        ruta.write_bytes(contenido)


@pytest.mark.parametrize("fallo", ["constructor", "observador", "sin_observador", "csv_distinto"])
def test_fallo_reconstruccion_no_deja_candidato_valido(inicial, monkeypatch, fallo):
    store, origen = inicial
    constructor = historico.construir_dataset_analitico

    def fallar(**opciones):
        assert opciones["ruta_sbs"].is_file() and opciones["ruta_bcrp"].is_file()
        assert opciones["ruta_sbs"].is_relative_to(store / "work")
        assert not opciones["ruta_sbs"].is_relative_to(origen)
        if fallo == "constructor":
            opciones["ruta_salida"].parent.mkdir(parents=True)
            opciones["ruta_salida"].write_text("parcial\n", encoding="utf-8")
            raise OSError("fallo del constructor")
        if fallo == "sin_observador":
            opciones.pop("observador_p75")
        resultado = constructor(**opciones)
        if fallo == "csv_distinto":
            alterado = resultado.copy(deep=True)
            alterado.loc[0, "morosidad_t"] += 1
            alterado.to_csv(opciones["ruta_salida"], index=False)
        return resultado

    def observador_fallido(datos, sbs):
        raise ValueError("fallo del observador")

    if fallo == "observador":
        monkeypatch.setattr(historico, "auditar_p75", observador_fallido)
    monkeypatch.setattr(historico, "construir_dataset_analitico", fallar)
    with pytest.raises((OSError, ValueError)):
        historico.reconstruir(store)
    fallidos = [ruta for ruta in (store / "work").iterdir() if ruta != origen]
    assert len(fallidos) == 1 and not (fallidos[0] / "manifest.json").exists()
    with pytest.raises(ValueError, match="incompleto"):
        historico.verificar(store, candidato=fallidos[0].name)
    historico.verificar(store)


def test_repeticion_copia_independiente_y_datos_equivalentes(reconstruido):
    store, origen, primero = reconstruido
    antes = inventario(primero)
    segundo = historico.reconstruir(store)
    assert len({origen.name, primero.name, segundo.name}) == 3
    assert inventario(primero) == antes
    for nuevo in (primero, segundo):
        historico.verificar(store, candidato=nuevo.name)
        comprobar_resultados_independientes(nuevo)
        for ruta in (historico.RECOMPUESTOS[historico.SBS], historico.RECOMPUESTOS[historico.BCRP]):
            assert not (nuevo / ruta).samefile(origen / ruta)
        assert not any(ruta.is_symlink() for ruta in nuevo.rglob("*"))
    pd.testing.assert_frame_equal(leer_features(primero / historico.FEATURES_RECONSTRUIDAS),
                                  leer_features(segundo / historico.FEATURES_RECONSTRUIDAS), check_exact=True)
    assert json_archivo(primero / historico.AUDITORIA_P75) == json_archivo(segundo / historico.AUDITORIA_P75)


@pytest.mark.parametrize("identificador", ["", ".", "..", "../initial", "/tmp/initial", "a/b", "a\\b", "á", "a" * 81])
def test_identificador_invalido(inicial, identificador):
    store, _ = inicial
    antes = inventario(store)
    for operacion in (historico.reconstruir, historico.verificar):
        with pytest.raises(ValueError, match="Identificador"):
            operacion(store, candidato=identificador)
    assert inventario(store) == antes


def test_candidato_enlace_fuera_de_work_se_rechaza(inicial, tmp_path):
    store, _ = inicial
    (store / "work/alias").symlink_to(tmp_path, target_is_directory=True)
    for operacion in (historico.reconstruir, historico.verificar):
        with pytest.raises(ValueError, match="work/"):
            operacion(store, candidato="alias")


@pytest.mark.parametrize("defecto", ["etapa", "extra", "hash", "origen", "inventario"])
def test_esquema_e_inventario_exactos(reconstruido, defecto):
    store, _, nuevo = reconstruido
    ruta = nuevo / "manifest.json"
    datos = json_archivo(ruta)
    if defecto == "etapa":
        datos["stage"] = "otra-etapa"
    elif defecto == "extra":
        datos["no_aprobado"] = True
    elif defecto == "hash":
        datos["source_candidate"]["manifest_sha256"] = "0" * 64
    elif defecto == "origen":
        datos["source_candidate"]["id"] = nuevo.name
    else:
        (nuevo / "extra.txt").write_text("inesperado", encoding="utf-8")
    escribir_json(ruta, datos)
    with pytest.raises(ValueError):
        historico.verificar(store, candidato=nuevo.name)


def test_rebuild_exige_candidate():
    with pytest.raises(SystemExit) as salida:
        historico.main(["rebuild", "--store", "/tmp/no-usado"])
    assert salida.value.code == 2


@pytest.mark.parametrize("criterio", ["22_meses", "incluye_2025"])
def test_rebuild_rechaza_p75_incorrecto_observado(inicial, monkeypatch, criterio):
    store, _ = inicial
    constructor = historico.construir_dataset_analitico
    sbs = pd.read_csv(REPOSITORIO / historico.SBS, float_precision=None)
    morosidad = sbs[sbs.banco_id.isin(BANCOS) & sbs.indicador_id.eq("Morosidad")]
    if criterio == "22_meses":
        morosidad = morosidad[morosidad.fecha_id.between("2023-03", "2024-12")]
    cortes = morosidad.groupby(CLAVES[1:]).valor.quantile(.75, interpolation="linear")

    def alterar_observacion(**opciones):
        observar = opciones["observador_p75"]

        def incorrecto(datos):
            datos["umbral_corte"] = [cortes.loc[banco, cartera]
                                     for banco, cartera in datos[CLAVES[1:]].itertuples(index=False, name=None)]
            observar(datos)

        opciones["observador_p75"] = incorrecto
        return constructor(**opciones)

    monkeypatch.setattr(historico, "construir_dataset_analitico", alterar_observacion)
    with pytest.raises(ValueError, match="P75 observado diferente"):
        historico.reconstruir(store)
    nuevo = next(ruta for ruta in (store / "work").iterdir() if ruta.name != "initial")
    assert not (nuevo / "manifest.json").exists()
    assert not (nuevo / historico.FEATURES_RECONSTRUIDAS).exists()


def test_initial_no_puede_declararse_reconstruccion(inicial):
    store, origen = inicial
    ruta = origen / "manifest.json"
    original = ruta.read_bytes()
    datos = json.loads(original)
    datos["stage"] = "historical-rebuild"
    datos["source_candidate"] = {"id": "initial", "manifest_sha256": "0" * 64}
    escribir_json(ruta, datos)
    try:
        with pytest.raises(ValueError, match="debe proceder"):
            historico.verificar(store)
    finally:
        ruta.write_bytes(original)
