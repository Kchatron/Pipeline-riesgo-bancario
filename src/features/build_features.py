"""
build_features.py — Construcción del Dataset Analítico para Machine Learning

Este módulo transforma los datos extraídos de SBS y BCRP:
1. Carga sbs_consolidado_2023_2025.csv y bcrp_macro_2023_2025.csv.
2. Excluye 'Total Banca Múltiple' del dataset de features (se mantiene solo para anclas/análisis agregado).
3. Pivotea de formato largo a ancho: una fila por (fecha_id, banco_id, tipo_credito_id).
4. Integra variables macroeconómicas del BCRP mediante join por fecha_id.
5. Genera rezagos temporales (lags t-1 y t-2) por panel (banco_id, tipo_credito_id) para evitar data leakage.
6. Define morosidad_t (target continuo) y riesgo_alto (target binario basado en el percentil 75 histórico por banco y cartera).
7. Exporta a data/processed/dataset_ml_features.csv.
"""

import os
import pandas as pd
import numpy as np


def construir_dataset_analitico(
    ruta_sbs="data/processed/sbs_consolidado_2023_2025.csv",
    ruta_bcrp="data/raw/BCRP/bcrp_macro_2023_2025.csv",
    ruta_salida="data/processed/dataset_ml_features.csv",
    umbral="p75"
):
    """
    Construye y guarda el dataset analítico para entrenamiento de modelos de ML.
    
    Parámetros:
    -----------
    ruta_sbs : str
        Ruta al consolidado procesado de la SBS.
    ruta_bcrp : str
        Ruta a las series macroeconómicas del BCRP (formato estrella o wide).
    ruta_salida : str
        Ruta de destino para dataset_ml_features.csv.
    umbral : str
        Criterio para el target binario 'riesgo_alto':
        - 'p75': percentil 75 histórico por banco y tipo de crédito (cuartil superior de morosidad).
        - 'mediana': mediana histórica por banco y tipo de crédito.
    """
    if not os.path.exists(ruta_sbs):
        raise FileNotFoundError(f"No se encontró el consolidado SBS en {ruta_sbs}")
    
    # 1. Cargar SBS
    df_sbs = pd.read_csv(ruta_sbs)
    
    # 2. Excluir 'Total Banca Múltiple' (agregado del sistema, no entidad par)
    bancos_individuales = ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco']
    df_sbs = df_sbs[df_sbs['banco_id'].isin(bancos_individuales)].copy()
    assert 'Total Banca Múltiple' not in df_sbs['banco_id'].values, (
        "Error crítico: 'Total Banca Múltiple' no debe formar parte del dataset analítico."
    )
    
    # 3. Pivotear SBS de formato largo a formato ancho
    df_pivot = df_sbs.pivot_table(
        index=['fecha_id', 'banco_id', 'tipo_credito_id'],
        columns='indicador_id',
        values='valor'
    ).reset_index()
    df_pivot.columns.name = None
    
    df_pivot = df_pivot.rename(columns={
        'Morosidad': 'morosidad_t',
        'Créditos Directos Totales': 'creditos_directos',
        'Tasa Activa MN': 'tasa_activa'
    })
    # Mantener 'morosidad' como alias de conveniencia
    df_pivot['morosidad'] = df_pivot['morosidad_t']
    
    # 4. Cargar e integrar BCRP
    if not os.path.exists(ruta_bcrp):
        ruta_bcrp_alt = "data/raw/BCRP/macro_bcrp_2023_2025.csv"
        if os.path.exists(ruta_bcrp_alt):
            ruta_bcrp = ruta_bcrp_alt
        else:
            raise FileNotFoundError(f"No se encontró archivo macro BCRP en {ruta_bcrp} ni en {ruta_bcrp_alt}")
            
    df_bcrp_raw = pd.read_csv(ruta_bcrp)
    if 'indicador_id' in df_bcrp_raw.columns and 'valor' in df_bcrp_raw.columns:
        df_bcrp = df_bcrp_raw.pivot(index='fecha_id', columns='indicador_id', values='valor').reset_index()
        df_bcrp.columns.name = None
    else:
        df_bcrp = df_bcrp_raw
        
    cols_macro = ['tasa_referencia', 'tipo_cambio', 'inflacion']
    for col in cols_macro:
        if col not in df_bcrp.columns:
            raise ValueError(f"Columna macroeconómica requerida '{col}' no encontrada en datos BCRP.")
            
    # Join por fecha_id (las variables macroeconómicas nacionales aplican a todos los bancos)
    df_full = pd.merge(df_pivot, df_bcrp[['fecha_id'] + cols_macro], on='fecha_id', how='left')
    df_full = df_full.sort_values(['banco_id', 'tipo_credito_id', 'fecha_id']).reset_index(drop=True)
    
    # 5. Generación de rezagos temporales (lags t-1 y t-2) por panel
    # Evita data leakage usando únicamente información del pasado inmediato
    cols_a_rezagar = [
        'morosidad_t', 'creditos_directos', 'tasa_activa',
        'tipo_cambio', 'tasa_referencia', 'inflacion'
    ]
    for c in cols_a_rezagar:
        base_name = 'morosidad' if c == 'morosidad_t' else c
        df_full[f'{base_name}_lag1'] = df_full.groupby(['banco_id', 'tipo_credito_id'])[c].shift(1)
        df_full[f'{base_name}_lag2'] = df_full.groupby(['banco_id', 'tipo_credito_id'])[c].shift(2)
        
    # 6. Definición de targets
    # Target continuo: morosidad_t (ya presente en el dataframe)
    # Target binario: riesgo_alto (1 si supera el umbral histórico por banco y tipo de crédito, 0 si no)
    # REGLA METODOLÓGICA ANTI-LEAKAGE: El umbral histórico (p75 o mediana) debe derivarse
    # ESTRICTAMENTE del periodo de entrenamiento (fecha_id <= '2024-12') para evitar que la
    # distribución del periodo de evaluación (test 2025) filtre la frontera de decisión.
    mascara_train = df_full['fecha_id'] <= '2024-12'
    
    if umbral == 'p75':
        cortes_train = (
            df_full[mascara_train]
            .groupby(['banco_id', 'tipo_credito_id'])['morosidad_t']
            .quantile(0.75)
            .reset_index()
            .rename(columns={'morosidad_t': 'umbral_corte'})
        )
    elif umbral == 'mediana':
        cortes_train = (
            df_full[mascara_train]
            .groupby(['banco_id', 'tipo_credito_id'])['morosidad_t']
            .median()
            .reset_index()
            .rename(columns={'morosidad_t': 'umbral_corte'})
        )
    else:
        raise ValueError(f"Umbral no reconocido: '{umbral}'. Use 'p75' o 'mediana'.")
        
    df_full = pd.merge(df_full, cortes_train, on=['banco_id', 'tipo_credito_id'], how='left')
    df_full['riesgo_alto'] = (df_full['morosidad_t'] > df_full['umbral_corte']).astype(int)
    df_full = df_full.drop(columns=['umbral_corte'])
    
    # 7. Filtrado de meses sin rezago completo (los dos primeros meses: 2023-01 y 2023-02)
    df_modelo = df_full.dropna().reset_index(drop=True)
    
    # Verificaciones de calidad
    assert 'Total Banca Múltiple' not in df_modelo['banco_id'].values, (
        "Error: 'Total Banca Múltiple' se detectó en el dataset analítico final."
    )
    assert df_modelo.isnull().sum().sum() == 0, (
        f"Error: Existen {df_modelo.isnull().sum().sum()} valores nulos en el dataset analítico."
    )
    
    # 8. Exportación
    os.makedirs(os.path.dirname(ruta_salida), exist_ok=True)
    df_modelo.to_csv(ruta_salida, index=False)
    
    print(f"✅ Dataset ML guardado en {ruta_salida}:")
    print(f"   - Filas: {df_modelo.shape[0]} (esperado: 340 = 34 meses × 5 bancos × 2 carteras)")
    print(f"   - Columnas: {df_modelo.shape[1]}")
    print(f"   - Total Banca Múltiple excluido: {'Total Banca Múltiple' not in df_modelo['banco_id'].values}")
    print(f"   - Target continuo: 'morosidad_t'")
    print(f"   - Target binario: 'riesgo_alto' (umbral: {umbral})")
    
    return df_modelo


if __name__ == "__main__":
    construir_dataset_analitico()