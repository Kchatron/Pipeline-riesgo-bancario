import io
import json
from pathlib import Path

import pandas as pd
import pytest

from src.data import bcrp_extractor


@pytest.fixture
def respuesta_bcrp(monkeypatch):
    payload = {
        'config': {'series': [
            {'name': 'PN01207PM'},
            {'name': 'PD04722MM'},
            {'name': 'PN01273PM'},
        ]},
        'periods': [
            {'name': 'Feb.2023', 'values': ['3.8', '7.5', '8.6']},
            {'name': 'Ene.2023', 'values': ['3.7', '7.75', '8.2']},
        ],
    }
    solicitudes = []

    def urlopen_simulado(request):
        solicitudes.append(request)
        return io.BytesIO(json.dumps(payload).encode('utf-8'))

    monkeypatch.setattr(bcrp_extractor.urllib.request, 'urlopen', urlopen_simulado)
    return solicitudes


@pytest.mark.parametrize('personalizado', [False, True])
def test_destino_y_contenido_csv(tmp_path, monkeypatch, respuesta_bcrp, personalizado):
    monkeypatch.chdir(tmp_path)
    if personalizado:
        destino = tmp_path / 'salidas' / 'generacion'
        resultado = bcrp_extractor.extraer_bcrp_api(
            bcrp_extractor.SERIES_BCRP, '2023-1', '2023-2',
            directorio_salida=destino
        )
        assert not (tmp_path / 'data').exists()
        fin = '2023-2'
    else:
        resultado = bcrp_extractor.extraer_bcrp_api()
        destino = tmp_path / 'data' / 'raw' / 'BCRP'
        fin = '2025-12'

    assert len(respuesta_bcrp) == 1
    assert respuesta_bcrp[0].full_url == (
        'https://estadisticas.bcrp.gob.pe/estadisticas/series/api/'
        f'PN01207PM-PD04722MM-PN01273PM/json/2023-1/{fin}'
    )
    assert respuesta_bcrp[0].get_header('User-agent') == 'Mozilla/5.0'

    esperado_wide = pd.DataFrame({
        'fecha_id': ['2023-01', '2023-02'],
        'tipo_cambio': [3.7, 3.8],
        'tasa_referencia': [7.75, 7.5],
        'inflacion': [8.2, 8.6],
    })
    esperado_long = pd.DataFrame({
        'fecha_id': ['2023-01', '2023-02'] * 3,
        'banco_id': ['Macro'] * 6,
        'tipo_credito_id': ['Macro'] * 6,
        'indicador_id': ['tipo_cambio'] * 2 + ['tasa_referencia'] * 2 + ['inflacion'] * 2,
        'valor': [3.7, 3.8, 7.75, 7.5, 8.2, 8.6],
    })
    wide = pd.read_csv(destino / 'macro_bcrp_2023_2025.csv')
    long = pd.read_csv(destino / 'bcrp_macro_2023_2025.csv')
    pd.testing.assert_frame_equal(resultado, esperado_wide, check_exact=True)
    pd.testing.assert_frame_equal(wide, esperado_wide, check_exact=True)
    pd.testing.assert_frame_equal(long, esperado_long, check_exact=True)

    reconstruido = long.pivot(index='fecha_id', columns='indicador_id', values='valor')
    reconstruido = reconstruido.rename_axis(columns=None).reset_index()
    pd.testing.assert_frame_equal(
        reconstruido[wide.columns], wide, check_exact=True
    )
    assert {path.name for path in destino.iterdir()} == {
        'macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'
    }


@pytest.mark.parametrize('archivo_fallido', [
    'macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'
])
@pytest.mark.parametrize('etapa', ['escritura', 'reemplazo'])
def test_fallo_conserva_archivo_y_limpia_temporales(
    tmp_path, monkeypatch, respuesta_bcrp, archivo_fallido, etapa
):
    monkeypatch.chdir(tmp_path)
    destino = tmp_path / 'salidas'
    destino.mkdir()
    for nombre in ('macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'):
        (destino / nombre).write_bytes(b'contenido anterior\n')

    to_csv_original = pd.DataFrame.to_csv
    replace_original = bcrp_extractor.os.replace

    def escribir_con_fallo(df, archivo, *args, **kwargs):
        es_long = 'indicador_id' in df.columns
        falla_long = archivo_fallido == 'bcrp_macro_2023_2025.csv'
        if es_long == falla_long:
            archivo.write('contenido parcial')
            raise OSError('fallo de escritura simulado')
        return to_csv_original(df, archivo, *args, **kwargs)

    def reemplazar_con_fallo(origen, ruta_final):
        if Path(ruta_final).name == archivo_fallido:
            raise OSError('fallo de reemplazo simulado')
        return replace_original(origen, ruta_final)

    if etapa == 'escritura':
        monkeypatch.setattr(pd.DataFrame, 'to_csv', escribir_con_fallo)
    else:
        monkeypatch.setattr(bcrp_extractor.os, 'replace', reemplazar_con_fallo)

    with pytest.raises(OSError, match=f'fallo de {etapa} simulado'):
        bcrp_extractor.extraer_bcrp_api(directorio_salida=destino)

    assert (destino / archivo_fallido).read_bytes() == b'contenido anterior\n'
    assert {path.name for path in destino.iterdir()} == {
        'macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'
    }
    assert not (tmp_path / 'data').exists()


def test_prepara_ambas_representaciones_antes_de_escribir(
    tmp_path, monkeypatch, respuesta_bcrp
):
    monkeypatch.chdir(tmp_path)
    destino = tmp_path / 'salidas'
    destino.mkdir()
    for nombre in ('macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'):
        (destino / nombre).write_bytes(b'contenido anterior\n')

    def fallar_preparacion(*args, **kwargs):
        raise ValueError('fallo de preparacion simulado')

    monkeypatch.setattr(pd.DataFrame, 'melt', fallar_preparacion)
    with pytest.raises(ValueError, match='fallo de preparacion simulado'):
        bcrp_extractor.extraer_bcrp_api(directorio_salida=destino)

    assert {path.name for path in destino.iterdir()} == {
        'macro_bcrp_2023_2025.csv', 'bcrp_macro_2023_2025.csv'
    }
    for archivo in destino.iterdir():
        assert archivo.read_bytes() == b'contenido anterior\n'
    assert not (tmp_path / 'data').exists()
