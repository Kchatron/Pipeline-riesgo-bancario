"""Validación del histórico conservando los tokens originales de los CSV."""

import csv
import io
from decimal import Decimal, InvalidOperation
from itertools import product


MESES = tuple(f"{anio}-{mes:02d}" for anio in (2023, 2024, 2025) for mes in range(1, 13))
BANCOS = ("BCP", "BBVA", "Interbank", "Scotiabank", "Mibanco")
CARTERAS = ("Consumo", "Hipotecario")
INDICADORES_SBS = ("Morosidad", "Créditos Directos Totales", "Tasa Activa MN")
INDICADORES_BCRP = ("tasa_referencia", "tipo_cambio", "inflacion")
COLUMNAS_HECHOS = ("fecha_id", "banco_id", "tipo_credito_id", "indicador_id", "valor")
COLUMNAS_MACRO = ("fecha_id", *INDICADORES_BCRP)
COLUMNAS_FEATURES = (
    "fecha_id", "banco_id", "tipo_credito_id", "creditos_directos", "morosidad_t",
    "tasa_activa", "morosidad", "tasa_referencia", "tipo_cambio", "inflacion",
    "morosidad_lag1", "morosidad_lag2", "creditos_directos_lag1", "creditos_directos_lag2",
    "tasa_activa_lag1", "tasa_activa_lag2", "tipo_cambio_lag1", "tipo_cambio_lag2",
    "tasa_referencia_lag1", "tasa_referencia_lag2", "inflacion_lag1", "inflacion_lag2",
    "riesgo_alto",
)
COLUMNAS_DECISIONES = (
    "morosidad_lag1", "morosidad_lag2", "tasa_activa_lag1", "tipo_cambio_lag1",
    "tasa_referencia_lag1", "inflacion_lag1", "banco_id", "tipo_credito_id",
    "riesgo_real", "prob_riesgo", "nivel_riesgo", "accion",
)


def leer_csv(contenido, columnas, nombre):
    """Valida el esquema; mantiene cada celda como texto, sin convertir números."""
    lector = csv.reader(io.StringIO(contenido.decode("utf-8"), newline=""))
    if tuple(next(lector, ())) != columnas:
        raise ValueError(f"Esquema incorrecto en {nombre}.")
    filas = []
    for numero, valores in enumerate(lector, start=2):
        if len(valores) != len(columnas) or any(valor == "" for valor in valores):
            raise ValueError(f"Fila incompleta en {nombre}:{numero}.")
        filas.append(dict(zip(columnas, valores)))
    return filas


def validar_numeros(filas, columnas, nombre):
    for numero, fila in enumerate(filas, start=2):
        for columna in columnas:
            try:
                valor = Decimal(fila[columna])
            except InvalidOperation as error:
                raise ValueError(f"Número inválido en {nombre}:{numero}, {columna}.") from error
            if not valor.is_finite():
                raise ValueError(f"Número no finito en {nombre}:{numero}, {columna}.")


def validar_claves(filas, columnas, esperadas, nombre):
    observadas = set()
    for fila in filas:
        clave = tuple(fila[columna] for columna in columnas)
        if clave in observadas:
            raise ValueError(f"Clave duplicada en {nombre}: {clave}.")
        if clave not in esperadas:
            raise ValueError(f"Clave no esperada en {nombre}: {clave}.")
        observadas.add(clave)
    faltantes = esperadas - observadas
    if faltantes:
        raise ValueError(f"Faltan {len(faltantes)} claves en {nombre}; primera: {min(faltantes)}.")


def leer_hechos(contenido, fuente, meses=MESES):
    filas = leer_csv(contenido, COLUMNAS_HECHOS, fuente)
    if fuente == "SBS":
        esperadas = set(product(meses, (*BANCOS, "Total Banca Múltiple"), CARTERAS, INDICADORES_SBS))
    elif fuente == "BCRP":
        esperadas = set(product(meses, ("Macro",), ("Macro",), INDICADORES_BCRP))
    else:
        raise ValueError(f"Fuente desconocida: {fuente}.")
    validar_claves(filas, COLUMNAS_HECHOS[:-1], esperadas, fuente)
    validar_numeros(filas, ("valor",), fuente)
    return filas


def leer_macro(contenido):
    filas = leer_csv(contenido, COLUMNAS_MACRO, "BCRP ancho")
    validar_claves(filas, ("fecha_id",), {(mes,) for mes in MESES}, "BCRP ancho")
    validar_numeros(filas, INDICADORES_BCRP, "BCRP ancho")
    return filas


def validar_features(contenido):
    filas = leer_csv(contenido, COLUMNAS_FEATURES, "Referencia de features")
    esperadas = set(product(MESES[2:], BANCOS, CARTERAS))
    validar_claves(filas, COLUMNAS_FEATURES[:3], esperadas, "Referencia de features")
    validar_numeros(filas, COLUMNAS_FEATURES[3:], "Referencia de features")
    if any(fila["riesgo_alto"] not in ("0", "1") for fila in filas):
        raise ValueError("riesgo_alto debe ser un entero binario.")


def validar_decisiones(contenido):
    filas = leer_csv(contenido, COLUMNAS_DECISIONES, "Referencia de decisiones")
    validar_numeros(filas, (*COLUMNAS_DECISIONES[:6], "riesgo_real", "prob_riesgo"), "Decisiones")
    if len(filas) != 120:
        raise ValueError("La referencia de decisiones debe tener 120 filas.")
    for fila in filas:
        if fila["banco_id"] not in BANCOS or fila["tipo_credito_id"] not in CARTERAS:
            raise ValueError("Entidad o cartera incorrecta en decisiones.")
        if fila["riesgo_real"] not in ("0", "1"):
            raise ValueError("riesgo_real debe ser un entero binario.")


def comparar_filas(referencia, candidato, claves, nombre):
    """Exige las mismas filas y tokens, alineados por claves; no añade tolerancias."""
    esperado = {tuple(fila[c] for c in claves): fila for fila in referencia}
    observado = {tuple(fila[c] for c in claves): fila for fila in candidato}
    if len(esperado) != len(referencia) or len(observado) != len(candidato):
        raise ValueError(f"Claves duplicadas al comparar {nombre}.")
    if esperado.keys() != observado.keys():
        raise ValueError(f"Claves diferentes al comparar {nombre}.")
    for clave, fila in esperado.items():
        for campo, valor in fila.items():
            if observado[clave].get(campo) != valor:
                raise ValueError(
                    f"Diferencia en {nombre}: clave={clave}, campo={campo}, "
                    f"referencia={valor}, candidato={observado[clave].get(campo)}."
                )


def macro_desde_largo(filas):
    valores = {(fila["fecha_id"], fila["indicador_id"]): fila["valor"] for fila in filas}
    return [
        {"fecha_id": mes, **{indicador: valores[mes, indicador] for indicador in INDICADORES_BCRP}}
        for mes in MESES
    ]
