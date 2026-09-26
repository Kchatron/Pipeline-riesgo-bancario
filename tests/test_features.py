"""
test_features.py — Pruebas Automatizadas para Feature Engineering y Anti-Leakage

Verifica:
1. Conteo de filas esperado tras el pivoteo (34 meses útiles × 5 bancos × 2 carteras = 340 filas),
   exclusión estricta de 'Total Banca Múltiple', ausencia de valores nulos y completitud de paneles.
2. Anti-leakage en rezagos temporales: verificación rigurosa de que morosidad_lag1(t) == morosidad(t-1)
   y morosidad_lag2(t) == morosidad(t-2), incluyendo contraste contra los meses 2023-01 y 2023-02
   del archivo consolidado de la SBS.
3. Anti-leakage en el umbral del target binario 'riesgo_alto': verificación matemática de que el
   percentil 75 (P75) fue calibrado ESTRICTAMENTE sobre datos de entrenamiento (fecha_id <= '2024-12'),
   protegiendo permanentemente el pipeline contra fugas de información del periodo de test 2025.
"""

import os
import pandas as pd
import numpy as np
import pytest


RUTA_DATASET_ML = "data/processed/dataset_ml_features.csv"
RUTA_SBS_CONSOLIDADO = "data/processed/sbs_consolidado_2023_2025.csv"


def test_conteo_filas_y_exclusion_total_banca_multiple():
    """Verifica dimensión, ausencia de agregados y completitud de paneles bancarios."""
    assert os.path.exists(RUTA_DATASET_ML), f"No existe el dataset en {RUTA_DATASET_ML}"
    df = pd.read_csv(RUTA_DATASET_ML)
    
    # 1. Conteo de filas esperado:
    # 36 meses - 2 meses descartados por lag t-2 = 34 meses útiles (2023-03 a 2025-12)
    # 5 bancos MVP × 2 carteras = 10 series de tiempo
    # 34 × 10 = 340 filas
    filas_esperadas = 340
    assert len(df) == filas_esperadas, f"Se esperaban {filas_esperadas} filas, pero se encontraron {len(df)}."
    
    # 2. Exclusión estricta de agregados del sistema
    assert "Total Banca Múltiple" not in df["banco_id"].values, (
        "Error crítico: 'Total Banca Múltiple' no debe estar presente en el dataset analítico de ML."
    )
    
    # 3. Bancos y carteras autorizados en el alcance del MVP
    bancos_esperados = {"BCP", "BBVA", "Interbank", "Scotiabank", "Mibanco"}
    assert set(df["banco_id"].unique()) == bancos_esperados, "Los bancos presentes no coinciden con el alcance MVP."
    
    carteras_esperadas = {"Consumo", "Hipotecario"}
    assert set(df["tipo_credito_id"].unique()) == carteras_esperadas, "Las carteras no coinciden con Consumo e Hipotecario."
    
    # 4. Ausencia total de valores nulos
    nulos = df.isnull().sum().sum()
    assert nulos == 0, f"Se encontraron {nulos} valores nulos en el dataset."
    
    # 5. Cada uno de los 10 paneles debe tener exactamente 34 observaciones continuas
    conteos_panel = df.groupby(["banco_id", "tipo_credito_id"]).size()
    assert (conteos_panel == 34).all(), "Cada panel (banco, cartera) debe contener exactamente 34 meses de datos."


def test_anti_leakage_rezagos_temporales():
    """Verifica que ningún lag use información contemporánea o futura (desfase temporal estricto)."""
    assert os.path.exists(RUTA_DATASET_ML), f"No existe {RUTA_DATASET_ML}"
    assert os.path.exists(RUTA_SBS_CONSOLIDADO), f"No existe {RUTA_SBS_CONSOLIDADO}"
    
    df_ml = pd.read_csv(RUTA_DATASET_ML)
    df_sbs = pd.read_csv(RUTA_SBS_CONSOLIDADO)
    
    for (banco, cartera), group in df_ml.groupby(["banco_id", "tipo_credito_id"]):
        group_sorted = group.sort_values("fecha_id").reset_index(drop=True)
        moro = group_sorted["morosidad_t"].values
        lag1 = group_sorted["morosidad_lag1"].values
        lag2 = group_sorted["morosidad_lag2"].values
        
        # 1. Verificar dentro de la serie del dataset analítico
        # lag1[i] debe ser moro[i-1] para i >= 1
        for i in range(1, len(moro)):
            assert np.isclose(lag1[i], moro[i - 1]), (
                f"Data leakage en lag1 para {banco} - {cartera} en fecha {group_sorted.loc[i, 'fecha_id']}: "
                f"lag1={lag1[i]} no coincide con morosidad_t-1={moro[i - 1]}."
            )
            
        # lag2[i] debe ser moro[i-2] para i >= 2
        for i in range(2, len(moro)):
            assert np.isclose(lag2[i], moro[i - 2]), (
                f"Data leakage en lag2 para {banco} - {cartera} en fecha {group_sorted.loc[i, 'fecha_id']}: "
                f"lag2={lag2[i]} no coincide con morosidad_t-2={moro[i - 2]}."
            )
            
        # 2. Verificar la frontera inicial (mes 2023-03) contra la fuente original SBS (2023-01 y 2023-02)
        row_marzo = group_sorted[group_sorted["fecha_id"] == "2023-03"].iloc[0]
        
        val_ene_sbs = df_sbs[
            (df_sbs["banco_id"] == banco) &
            (df_sbs["tipo_credito_id"] == cartera) &
            (df_sbs["indicador_id"] == "Morosidad") &
            (df_sbs["fecha_id"] == "2023-01")
        ]["valor"].values[0]
        
        val_feb_sbs = df_sbs[
            (df_sbs["banco_id"] == banco) &
            (df_sbs["tipo_credito_id"] == cartera) &
            (df_sbs["indicador_id"] == "Morosidad") &
            (df_sbs["fecha_id"] == "2023-02")
        ]["valor"].values[0]
        
        assert np.isclose(row_marzo["morosidad_lag1"], val_feb_sbs), (
            f"Frontera inicial lag1 desalineada para {banco} {cartera} en 2023-03 respecto a Feb-2023 SBS."
        )
        assert np.isclose(row_marzo["morosidad_lag2"], val_ene_sbs), (
            f"Frontera inicial lag2 desalineada para {banco} {cartera} en 2023-03 respecto a Ene-2023 SBS."
        )


def test_anti_leakage_umbral_p75_exclusivo_train():
    """
    Compuerta de Calidad Permanente Anti-Leakage de Target:
    Verifica que el umbral P75 de 'riesgo_alto' fue derivado ÚNICAMENTE con datos de Train (fecha_id <= '2024-12').
    Atrapa automáticamente cualquier regresión o fuga del periodo de test 2025.
    """
    assert os.path.exists(RUTA_DATASET_ML), f"No existe {RUTA_DATASET_ML}"
    df = pd.read_csv(RUTA_DATASET_ML)
    
    mascara_train = df["fecha_id"] <= "2024-12"
    
    # Calcular el P75 histórico canónico derivado estrictamente de Train
    cortes_train_esperados = (
        df[mascara_train]
        .groupby(["banco_id", "tipo_credito_id"])["morosidad_t"]
        .quantile(0.75)
        .to_dict()
    )
    
    # Calcular el P75 hipotético contaminado (calculado sobre los 36 meses completos)
    cortes_full_contaminados = (
        df.groupby(["banco_id", "tipo_credito_id"])["morosidad_t"]
        .quantile(0.75)
        .to_dict()
    )
    
    # 1. Comprobar que en cada fila, riesgo_alto coincide 100% con el umbral exclusivo de Train
    discrepancias_train = []
    for idx, row in df.iterrows():
        clave = (row["banco_id"], row["tipo_credito_id"])
        umbral_val = cortes_train_esperados[clave]
        etiqueta_esperada = int(row["morosidad_t"] > umbral_val)
        
        if row["riesgo_alto"] != etiqueta_esperada:
            discrepancias_train.append((row["fecha_id"], row["banco_id"], row["tipo_credito_id"], row["riesgo_alto"], etiqueta_esperada))
            
    assert len(discrepancias_train) == 0, (
        f"Falla crítica de integridad: Se detectaron {len(discrepancias_train)} filas donde 'riesgo_alto' "
        f"no coincide con el P75 derivado de Train. Muestra: {discrepancias_train[:3]}"
    )
    
    # 2. Demostración de sensibilidad: Verificar que si se hubiera usado el P75 contaminado (full),
    # existirían discrepancias reales en las etiquetas (demuestra que la compuerta es efectiva).
    discrepancias_con_full = 0
    for idx, row in df.iterrows():
        clave = (row["banco_id"], row["tipo_credito_id"])
        umbral_full = cortes_full_contaminados[clave]
        etiqueta_full = int(row["morosidad_t"] > umbral_full)
        if row["riesgo_alto"] != etiqueta_full:
            discrepancias_con_full += 1
            
    assert discrepancias_con_full > 0, (
        "La prueba no pudo demostrar sensibilidad: los cortes de train y full son idénticos, "
        "lo que impediría detectar una fuga futura."
    )
