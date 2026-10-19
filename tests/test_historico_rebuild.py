"""Reconstrucción aislada: float64 exacto y CSV round_trip, sin tolerancias."""

import csv
import hashlib
from itertools import product
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.features.build_features import construir_dataset_analitico
from src.pipeline import historico
from src.pipeline.validacion import (
    BANCOS,
    CARTERAS,
    COLUMNAS_FEATURES,
    COLUMNAS_HECHOS,
    MESES,
)


REPOSITORIO = Path(__file__).resolve().parents[1]
CLAVES = ['fecha_id', 'banco_id', 'tipo_credito_id']
PANELES = ['banco_id', 'tipo_credito_id']
COLUMNAS_OBSERVADOR = [*CLAVES, 'morosidad_t', 'umbral_corte']
TIPOS_NUMERICOS = {columna: 'float64' for columna in COLUMNAS_FEATURES[3:-1]}
TIPOS_NUMERICOS['riesgo_alto'] = 'int64'


def inventario(raiz):
    return {
        ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
        for ruta in raiz.rglob('*') if ruta.is_file()
    }


def leer_features(ruta):
    return pd.read_csv(ruta, float_precision='round_trip', dtype=TIPOS_NUMERICOS)


def reconstruir(ruta_sbs, ruta_bcrp, salida, **opciones):
    for entrada in (ruta_sbs, ruta_bcrp):
        if not entrada.is_file():
            raise FileNotFoundError(f'Entrada requerida antes del constructor: {entrada}')
    return construir_dataset_analitico(
        ruta_sbs=ruta_sbs, ruta_bcrp=ruta_bcrp, ruta_salida=salida, **opciones
    )


def comprobar_golden(candidato, referencia):
    assert candidato.shape == (340, 23)
    assert list(candidato.columns) == list(COLUMNAS_FEATURES) == list(referencia.columns)
    assert not candidato.isna().any().any()
    assert not candidato.duplicated(CLAVES).any()
    assert set(candidato.riesgo_alto) <= {0, 1}
    assert candidato.riesgo_alto.dtype == np.dtype('int64')
    assert 'Total Banca Múltiple' not in set(candidato.banco_id)
    assert set(candidato[CLAVES].itertuples(index=False, name=None)) == set(
        product(MESES[2:], BANCOS, CARTERAS)
    )
    assert candidato.groupby(PANELES).size().eq(34).all()
    actual = candidato.set_index(CLAVES).sort_index()
    esperado = referencia.set_index(CLAVES).sort_index()
    pd.testing.assert_index_equal(actual.index, esperado.index)
    pd.testing.assert_series_equal(actual.dtypes, esperado.dtypes)
    diferencias = actual.ne(esperado)
    if diferencias.any().any():
        detalles = []
        for campo in diferencias.columns[diferencias.any()]:
            mascara = diferencias[campo]
            delta = actual.loc[mascara, campo] - esperado.loc[mascara, campo]
            ejemplos = [
                {
                    'clave': clave, 'campo': campo,
                    'referencia': repr(esperado.loc[clave, campo]),
                    'candidato': repr(actual.loc[clave, campo]),
                    'diferencia': repr(actual.loc[clave, campo] - esperado.loc[clave, campo]),
                }
                for clave in actual.index[mascara][:3]
            ]
            detalles.append({
                'campo': campo, 'cantidad': int(mascara.sum()),
                'max_abs': repr(delta.abs().max()), 'ejemplos': ejemplos,
            })
        raise AssertionError(f'Golden master diferente, sin tolerancias: {detalles}')
    pd.testing.assert_frame_equal(actual, esperado, check_exact=True)


def poblacion_referencia(ruta_sbs):
    # float_precision=None reproduce la lectura numérica actual del constructor.
    # No se usa el CSV de features ni los cortes observados para calcular el P75.
    sbs = pd.read_csv(ruta_sbs, float_precision=None)
    return sbs[sbs.banco_id.isin(BANCOS) & sbs.indicador_id.eq('Morosidad')].copy()


def comprobar_p75(observado, ruta_sbs):
    assert list(observado.columns) == COLUMNAS_OBSERVADOR
    assert len(observado) == 360
    assert not observado.isna().any().any()
    assert not observado.duplicated(CLAVES).any()
    train_observado = observado[observado.fecha_id.between('2023-01', '2024-12')]
    assert train_observado.groupby(PANELES).size().eq(24).all()
    assert set(train_observado.fecha_id) == set(MESES[:24])
    fuente = poblacion_referencia(ruta_sbs)
    pd.testing.assert_series_equal(
        observado.set_index(CLAVES).sort_index().morosidad_t,
        fuente.rename(columns={'valor': 'morosidad_t'}).set_index(CLAVES).sort_index().morosidad_t,
        check_exact=True,
    )
    train = fuente[fuente.fecha_id.between('2023-01', '2024-12')]
    assert len(train) == 240
    assert train.groupby(PANELES).size().eq(24).all()
    esperado = train.groupby(PANELES).valor.quantile(.75, interpolation='linear')
    esperado = esperado.rename('umbral_corte')
    assert len(esperado) == 10
    assert observado.groupby(PANELES).umbral_corte.nunique().eq(1).all()
    actual = observado.groupby(PANELES).umbral_corte.first()
    pd.testing.assert_series_equal(actual, esperado, check_exact=True)


def comprobar_etiquetas(resultado, observado):
    actual = resultado.set_index(CLAVES).sort_index().riesgo_alto
    usados = observado.set_index(CLAVES).loc[actual.index]
    esperado = (usados.morosidad_t > usados.umbral_corte).astype('int64')
    esperado = esperado.rename('riesgo_alto')
    pd.testing.assert_series_equal(actual, esperado, check_exact=True)


@pytest.fixture(autouse=True)
def referencias_intactas():
    manifiesto = json.loads((REPOSITORIO / historico.MANIFIESTO_REFERENCIAS).read_bytes())

    def comprobar():
        for entrada in manifiesto['references']:
            assert hashlib.sha256((REPOSITORIO / entrada['path']).read_bytes()).hexdigest() == entrada['sha256']

    comprobar()
    yield
    comprobar()


@pytest.fixture
def candidato_verificado(tmp_path):
    store = tmp_path / 'store'
    candidato = historico.inicializar(store)
    historico.verificar(store)
    antes = inventario(candidato)
    ruta_sbs = candidato / historico.RECOMPUESTOS[historico.SBS]
    ruta_bcrp = candidato / historico.RECOMPUESTOS[historico.BCRP]
    assert ruta_sbs.is_file() and ruta_bcrp.is_file()
    yield candidato, ruta_sbs, ruta_bcrp
    assert inventario(candidato) == antes
    historico.verificar(store)


@pytest.fixture
def reconstruccion(candidato_verificado, tmp_path):
    candidato, ruta_sbs, ruta_bcrp = candidato_verificado
    capturas = []
    salida = tmp_path / 'reconstruccion' / 'features.csv'
    assert not salida.is_relative_to(candidato)
    resultado = reconstruir(ruta_sbs, ruta_bcrp, salida, observador_p75=capturas.append)
    assert len(capturas) == 1
    return resultado, salida, capturas[0]


def test_golden_master_dataframe_y_csv(reconstruccion):
    resultado, salida, _ = reconstruccion
    referencia = leer_features(REPOSITORIO / historico.FEATURES)
    comprobar_golden(resultado, referencia)
    comprobar_golden(leer_features(salida), referencia)


def test_p75_real_observado_y_etiquetas(reconstruccion):
    resultado, _, observado = reconstruccion
    comprobar_p75(observado, REPOSITORIO / historico.SBS)
    comprobar_etiquetas(resultado, observado)


def test_resultado_igual_sin_observador(reconstruccion, candidato_verificado, tmp_path):
    con_observador, salida_con, _ = reconstruccion
    _, ruta_sbs, ruta_bcrp = candidato_verificado
    salida_sin = tmp_path / 'sin_observador' / 'features.csv'
    sin_observador = reconstruir(ruta_sbs, ruta_bcrp, salida_sin)
    pd.testing.assert_frame_equal(con_observador, sin_observador, check_exact=True)
    assert salida_con.read_bytes() == salida_sin.read_bytes()


def test_mutar_copia_no_altera_constructor(reconstruccion, candidato_verificado, tmp_path):
    esperado, salida_original, _ = reconstruccion
    _, ruta_sbs, ruta_bcrp = candidato_verificado
    llamadas = []

    def modificar(copia):
        llamadas.append(True)
        assert list(copia.columns) == COLUMNAS_OBSERVADOR
        copia.loc[:, 'morosidad_t'] = -999.0
        copia.loc[:, 'umbral_corte'] = -999.0
        copia.loc[:, 'fecha_id'] = '1900-01'
        copia.loc[:, 'banco_id'] = 'Otro'
        copia.loc[:, 'tipo_credito_id'] = 'Otra'
        copia.drop(columns='umbral_corte', inplace=True)

    salida = tmp_path / 'observador_mutante' / 'features.csv'
    resultado = reconstruir(ruta_sbs, ruta_bcrp, salida, observador_p75=modificar)
    assert llamadas == [True]
    pd.testing.assert_frame_equal(resultado, esperado, check_exact=True)
    assert salida.read_bytes() == salida_original.read_bytes()


def test_error_del_observador_se_propaga(candidato_verificado, tmp_path):
    _, ruta_sbs, ruta_bcrp = candidato_verificado
    salida = tmp_path / 'fallo_observador' / 'features.csv'

    def fallar(copia):
        raise RuntimeError('fallo del observador')

    with pytest.raises(RuntimeError, match='fallo del observador'):
        reconstruir(ruta_sbs, ruta_bcrp, salida, observador_p75=fallar)
    assert not salida.exists()


@pytest.fixture
def fuentes_sinteticas(tmp_path):
    ruta_sbs = tmp_path / 'sbs_sintetico.csv'
    valores = [float(numero) for numero in range(1, 25)] + [18.5] + list(range(50, 61))
    with ruta_sbs.open('w', encoding='utf-8', newline='') as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=COLUMNAS_HECHOS)
        escritor.writeheader()
        for banco, cartera, (mes, valor) in product(BANCOS, CARTERAS, zip(MESES, valores)):
            for indicador, numero in (
                ('Morosidad', valor), ('Créditos Directos Totales', 1000.0), ('Tasa Activa MN', 10.0)
            ):
                escritor.writerow(dict(zip(COLUMNAS_HECHOS, (mes, banco, cartera, indicador, numero))))
    ruta_macro = tmp_path / 'macro_sintetico.csv'
    with ruta_macro.open('w', encoding='utf-8', newline='') as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(['fecha_id', 'tasa_referencia', 'tipo_cambio', 'inflacion'])
        escritor.writerows((mes, 7.0, 3.5, 2.0) for mes in MESES)
    return ruta_sbs, ruta_macro


@pytest.mark.parametrize('criterio_incorrecto', ['22_meses', 'incluye_2025'])
def test_criterio_correcto_rechaza_cortes_y_etiquetas_incorrectos(
    fuentes_sinteticas, tmp_path, criterio_incorrecto
):
    ruta_sbs, ruta_bcrp = fuentes_sinteticas
    capturas = []
    resultado = reconstruir(
        ruta_sbs, ruta_bcrp, tmp_path / 'sintetico' / 'features.csv',
        observador_p75=capturas.append
    )
    observado = capturas[0]
    comprobar_p75(observado, ruta_sbs)
    comprobar_etiquetas(resultado, observado)
    fuente = poblacion_referencia(ruta_sbs)
    if criterio_incorrecto == '22_meses':
        incorrecta = fuente[fuente.fecha_id.between('2023-03', '2024-12')]
    else:
        incorrecta = fuente
    cortes_incorrectos = incorrecta.groupby(PANELES).valor.quantile(.75, interpolation='linear')
    correctos = observado.groupby(PANELES).umbral_corte.first()
    assert cortes_incorrectos.ne(correctos).all()
    copia = observado.copy(deep=True)
    copia['umbral_corte'] = pd.MultiIndex.from_frame(copia[PANELES]).map(cortes_incorrectos)
    with pytest.raises(AssertionError):
        comprobar_p75(copia, ruta_sbs)
    indices = pd.MultiIndex.from_frame(resultado[PANELES])
    etiquetas_incorrectas = (resultado.morosidad_t > np.asarray(indices.map(cortes_incorrectos))).astype('int64')
    assert etiquetas_incorrectas.ne(resultado.riesgo_alto).any()
    resultado_incorrecto = resultado.copy(deep=True)
    resultado_incorrecto['riesgo_alto'] = etiquetas_incorrectas
    with pytest.raises(AssertionError):
        comprobar_etiquetas(resultado_incorrecto, observado)
