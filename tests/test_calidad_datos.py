import os
import pandas as pd
import numpy as np

def test_calidad():
    ruta_df = "data/processed/sbs_consolidado_2023_2025.csv"
    assert os.path.exists(ruta_df), "El archivo procesado consolidado no existe."
    df = pd.read_csv(ruta_df)
    
    # 1. Total exacto y nulos
    assert len(df) == 1296, f"Se esperaban 1296 filas, hay {len(df)}"
    assert df.isnull().sum().sum() == 0, "Existen valores nulos en el consolidado"
    
    # 2. Control anclas de tasas activas
    anclas = {'2023-01': 49.82, '2024-01': 57.45, '2025-01': 60.43}
    for periodo, val_esp in anclas.items():
        filtro = (
            (df['fecha_id'] == periodo) & 
            (df['banco_id'] == 'Total Banca Múltiple') & 
            (df['tipo_credito_id'] == 'Consumo') & 
            (df['indicador_id'] == 'Tasa Activa MN')
        )
        val_real = df[filtro]['valor'].values[0]
        assert np.isclose(val_real, val_esp, atol=0.05), f"Ancla {periodo} desalineada."
        
    print("✅ Todas las pruebas de calidad (DQ) pasaron exitosamente.")

if __name__ == "__main__":
    test_calidad()