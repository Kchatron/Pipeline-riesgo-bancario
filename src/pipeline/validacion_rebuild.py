"""Contrato numérico exacto de reconstrucción y evidencia del P75 observado."""

import io
from itertools import product

import numpy as np
import pandas as pd

from src.pipeline.validacion import BANCOS, CARTERAS, COLUMNAS_FEATURES, MESES, validar_features


CLAVES = list(COLUMNAS_FEATURES[:3])
PANELES = CLAVES[1:]
COLUMNAS_OBSERVADOR = [*CLAVES, "morosidad_t", "umbral_corte"]
TIPOS = {columna: "float64" for columna in COLUMNAS_FEATURES[3:-1]}
TIPOS["riesgo_alto"] = "int64"


def leer_features_exactas(contenido):
    """CSV: float64 round_trip e int64 binario, sin redondeos ni tolerancias."""
    validar_features(contenido)
    return pd.read_csv(io.BytesIO(contenido), float_precision="round_trip", dtype=TIPOS)


def comparar_features(resultado, referencia):
    esperado = leer_features_exactas(referencia).set_index(CLAVES).sort_index()
    if resultado.shape != (340, 23) or tuple(resultado.columns) != COLUMNAS_FEATURES:
        raise ValueError("Features: se requieren 340 filas y las 23 columnas en su orden aprobado.")
    if resultado.isna().any().any() or resultado.duplicated(CLAVES).any():
        raise ValueError("Features: nulos o claves duplicadas.")
    claves = set(resultado[CLAVES].itertuples(index=False, name=None))
    if claves != set(product(MESES[2:], BANCOS, CARTERAS)):
        raise ValueError("Features: calendario, paneles o exclusión del agregado incorrectos.")
    for campo, tipo in TIPOS.items():
        if resultado[campo].dtype != np.dtype(tipo) or not np.isfinite(resultado[campo]).all():
            raise ValueError(f"Features: tipo o número no finito en {campo}.")
    if not resultado.riesgo_alto.isin((0, 1)).all():
        raise ValueError("Features: etiquetas no binarias.")
    actual = resultado.set_index(CLAVES).sort_index()
    diferencias = actual.ne(esperado)
    if diferencias.any().any():
        detalles = []
        for campo in diferencias.columns[diferencias.any()]:
            mascara = diferencias[campo]
            delta = actual.loc[mascara, campo] - esperado.loc[mascara, campo]
            ejemplos = [
                {"clave": clave, "campo": campo,
                 "referencia": repr(float(esperado.loc[clave, campo])),
                 "candidato": repr(float(actual.loc[clave, campo])),
                 "diferencia": repr(float(delta.loc[clave]))}
                for clave in actual.index[mascara][:3]
            ]
            detalles.append({"campo": campo, "cantidad": int(mascara.sum()),
                             "max_abs": repr(float(delta.abs().max())), "ejemplos": ejemplos})
        raise ValueError(f"Features diferentes, sin tolerancias: {detalles}")


def _poblacion(sbs):
    # La lectura None reproduce la entrada numérica del constructor existente.
    fuente = pd.read_csv(io.BytesIO(sbs), float_precision=None)
    fuente = fuente[fuente.banco_id.isin(BANCOS) & fuente.indicador_id.eq("Morosidad")]
    fuente = fuente.rename(columns={"valor": "morosidad_t"})
    claves = set(fuente[CLAVES].itertuples(index=False, name=None))
    if len(fuente) != 360 or claves != set(product(MESES, BANCOS, CARTERAS)):
        raise ValueError("P75: población SBS incompleta o duplicada.")
    if not np.isfinite(fuente.morosidad_t).all():
        raise ValueError("P75: morosidad no finita.")
    train = fuente[fuente.fecha_id.between("2023-01", "2024-12")]
    if len(train) != 240 or not train.groupby(PANELES).size().eq(24).all():
        raise ValueError("P75: se requieren 24 observaciones de entrenamiento por panel.")
    cortes = train.groupby(PANELES).morosidad_t.quantile(.75, interpolation="linear")
    return fuente, train, cortes


def _auditoria(train, cortes, observados):
    paneles = []
    for (banco, cartera), corte in cortes.items():
        filas = train[train.banco_id.eq(banco) & train.tipo_credito_id.eq(cartera)].sort_values("fecha_id")
        paneles.append({
            "banco_id": banco, "tipo_credito_id": cartera,
            "observed_cut": float(observados.loc[banco, cartera]),
            "expected_cut": float(corte), "count": len(filas),
            "population": [{"fecha_id": fila.fecha_id, "morosidad_t": float(fila.morosidad_t)}
                           for fila in filas.itertuples()],
            "equal": True,
        })
    return {
        "schema_version": 1, "quantile": .75, "interpolation": "linear",
        "input_float_precision": None, "numeric_type": "float64",
        "training_start": "2023-01", "training_end": "2024-12",
        "comparison": "exact", "equal": True, "panels": paneles,
    }


def auditar_p75(observado, sbs):
    """Comprueba la captura real antes de aceptar la auditoría y las etiquetas."""
    fuente, train, cortes = _poblacion(sbs)
    if list(observado.columns) != COLUMNAS_OBSERVADOR or len(observado) != 360:
        raise ValueError("P75 observado: esquema o cantidad incorrectos.")
    if observado.isna().any().any() or observado.duplicated(CLAVES).any():
        raise ValueError("P75 observado: nulos o duplicados.")
    actual = observado.set_index(CLAVES).sort_index()
    esperado = fuente.set_index(CLAVES).sort_index()
    if not actual.index.equals(esperado.index) or not actual.morosidad_t.equals(esperado.morosidad_t):
        raise ValueError("P75 observado: población diferente de la referencia SBS.")
    if not np.isfinite(actual.umbral_corte).all():
        raise ValueError("P75 observado: corte no finito.")
    if not observado.groupby(PANELES).umbral_corte.nunique().eq(1).all():
        raise ValueError("P75 observado: cortes variables dentro del panel.")
    usados = observado.groupby(PANELES).umbral_corte.first()
    if not usados.index.equals(cortes.index) or not usados.eq(cortes).all():
        raise ValueError(f"P75 observado diferente del criterio de 24 meses: {(usados - cortes).to_dict()}")
    return _auditoria(train, cortes, usados)


def verificar_auditoria(auditoria, sbs):
    """Recalcula población y cortes; no reejecuta el constructor al verificar."""
    _, train, cortes = _poblacion(sbs)
    if auditoria != _auditoria(train, cortes, cortes):
        raise ValueError("Auditoría P75 alterada o incompatible con la población y los cortes esperados.")


def verificar_cortes_referencia(auditoria, sbs_referencia):
    """Compara cortes, sin sustituir la población del candidato por la referencia."""
    _, _, cortes = _poblacion(sbs_referencia)
    diferencias = []
    for panel in auditoria["panels"]:
        clave = panel["banco_id"], panel["tipo_credito_id"]
        referencia = float(cortes.loc[clave])
        if panel["expected_cut"] != referencia or panel["observed_cut"] != referencia:
            diferencias.append({"panel": clave, "reference_cut": referencia,
                                "expected_cut": panel["expected_cut"],
                                "observed_cut": panel["observed_cut"],
                                "difference": panel["observed_cut"] - referencia})
    if diferencias:
        raise ValueError(f"P75 del candidato diferente de los cortes de referencia: {diferencias}")


def verificar_etiquetas(resultado, auditoria):
    cortes = {(panel["banco_id"], panel["tipo_credito_id"]): panel["observed_cut"]
              for panel in auditoria["panels"]}
    limites = np.array([cortes[clave] for clave in resultado[PANELES].itertuples(index=False, name=None)])
    if not resultado.riesgo_alto.eq((resultado.morosidad_t > limites).astype("int64")).all():
        raise ValueError("Etiquetas diferentes de morosidad_t > P75 observado.")
