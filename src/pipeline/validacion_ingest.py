"""Validación mensual y comparaciones numéricas, sin imponer tokens históricos."""

from decimal import Decimal, localcontext

from src.pipeline.validacion import (
    COLUMNAS_MACRO,
    INDICADORES_BCRP,
    MESES,
    leer_csv,
    leer_hechos,
    validar_claves,
    validar_numeros,
)


COLUMNAS_API = ("fecha_id", "tipo_cambio", "tasa_referencia", "inflacion")
REGLA_SBS = "Decimal(candidate) == Decimal(reference)"
REGLA_BCRP = "abs(Decimal(candidate) - Decimal(reference)) <= 1e-9; rtol=0"


def validar_mes(mes):
    if mes not in MESES:
        raise ValueError("--month debe ser YYYY-MM entre 2023-01 y 2025-12.")


def leer_ancho(contenido, meses, columnas=COLUMNAS_MACRO):
    filas = leer_csv(contenido, columnas, "BCRP ancho")
    validar_claves(filas, ("fecha_id",), {(mes,) for mes in meses}, "BCRP ancho")
    validar_numeros(filas, INDICADORES_BCRP, "BCRP ancho")
    return filas


def diferencia_exacta(candidato, referencia):
    a, b = Decimal(candidato), Decimal(referencia)
    # Evita que el contexto Decimal por defecto redondee la resta de tokens largos.
    with localcontext() as contexto:
        contexto.prec = max(len(a.as_tuple().digits), len(b.as_tuple().digits)) + abs(
            a.as_tuple().exponent - b.as_tuple().exponent
        ) + 2
        return a - b


def comparar_valores(referencia, candidato, conjunto, contra, tolerancia=False):
    """Ambas poblaciones deben tener previamente esquema y claves validados."""
    campos = ("fecha_id", "banco_id", "tipo_credito_id", "indicador_id")
    esperado = {tuple(fila[campo] for campo in campos): fila["valor"] for fila in referencia}
    observado = {tuple(fila[campo] for campo in campos): fila["valor"] for fila in candidato}
    if esperado.keys() != observado.keys() or len(esperado) != len(referencia) or len(observado) != len(candidato):
        raise ValueError(f"Claves diferentes o duplicadas al comparar {conjunto}.")
    informe = []
    for clave in sorted(esperado):
        referencia, candidato = esperado[clave], observado[clave]
        delta = diferencia_exacta(candidato, referencia)
        aceptado = delta.copy_abs() <= Decimal("1e-9") if tolerancia else delta == 0
        informe.append({
            "dataset": conjunto, "against": contra, "key": dict(zip(campos, clave)),
            "field": "valor", "reference": referencia, "candidate": candidato,
            "difference": str(delta), "rule": REGLA_BCRP if tolerancia else REGLA_SBS,
            "tokens_equal": candidato == referencia, "numeric_equal": delta == 0,
            "accepted": aceptado,
        })
    return informe


def coherencia_bcrp(largo, ancho):
    desde_ancho = [
        {"fecha_id": fila["fecha_id"], "banco_id": "Macro", "tipo_credito_id": "Macro",
         "indicador_id": indicador, "valor": fila[indicador]}
        for fila in ancho for indicador in INDICADORES_BCRP
    ]
    return comparar_valores(desde_ancho, largo, "BCRP", "wide/long", tolerancia=False)


def comparar_mes(mes, sbs, bcrp, ancho, referencias, origen):
    """Registra todas las comparaciones, incluidas diferencias BCRP aceptadas."""
    entradas = {"SBS": sbs, "BCRP": bcrp}
    comparaciones = coherencia_bcrp(bcrp, ancho)
    for nombre, fuentes in (("approved-reference", referencias), ("initial", origen)):
        for conjunto, filas in entradas.items():
            base = [fila for fila in fuentes[conjunto] if fila["fecha_id"] == mes]
            comparaciones.extend(comparar_valores(base, filas, conjunto, nombre, conjunto == "BCRP"))
    return {
        "schema_version": 1, "stage": "historical-ingest", "month": mes,
        "status": "matched" if all(fila["accepted"] for fila in comparaciones) else "blocked",
        "comparisons": comparaciones,
    }


def leer_mes(sbs, bcrp, ancho, mes):
    return (
        leer_hechos(sbs, "SBS", meses=(mes,)),
        leer_hechos(bcrp, "BCRP", meses=(mes,)),
        leer_ancho(ancho, (mes,), COLUMNAS_API),
    )
