import os
import glob
import re
import pandas as pd

MAPEO_BANCOS = {
    'b. de crédito del perú': 'BCP',
    'banco de crédito': 'BCP',
    'bcp': 'BCP',
    'b. bbva perú': 'BBVA',
    'bbva': 'BBVA',
    'interbank': 'Interbank',
    'scotiabank perú': 'Scotiabank',
    'scotiabank': 'Scotiabank',
    'mibanco': 'Mibanco',
    'total banca múltiple': 'Total Banca Múltiple',
    'promedio': 'Total Banca Múltiple',
    'b. de comercio': 'Bancom',
    'bancom': 'Bancom'
}

def normalizar_nombre_banco(nombre_crudo):
    """Convierte el nombre extraído al identificador estándar del esquema estrella."""
    nombre_limpio = str(nombre_crudo).lower().strip()
    if nombre_limpio in ['crédito', 'credito']:
        return 'BCP'
    for clave, valor_estandar in MAPEO_BANCOS.items():
        if clave in nombre_limpio:
            return valor_estandar
    return None

def validar_integridad_temporal(df, ruta_archivo, periodo, max_filas=6):
    """Valida que el año esperado figure explícitamente en la cabecera del Excel."""
    anio_esperado = str(periodo.split('-')[0])
    textos_cabecera = [str(x) for x in df.iloc[0:max_filas].values.flatten() if pd.notna(x)]
    if not any(anio_esperado in texto for texto in textos_cabecera):
        raise ValueError(
            f"Alerta temporal: Se esperaba el año {anio_esperado} para el periodo {periodo}, "
            f"pero no figura en la cabecera de {ruta_archivo}."
        )

def extraer_morosidad(ruta_archivo, periodo):
    """Extrae índices de morosidad (B-2362)."""
    df = pd.read_excel(ruta_archivo, sheet_name=0, header=None)
    validar_integridad_temporal(df, ruta_archivo, periodo)
    
    encabezados = df.iloc[5]
    fila_consumo, fila_hipo = None, None
    for idx, val in enumerate(df[0]):
        if pd.notna(val):
            texto = str(val).lower().strip()
            if 'créditos de consumo' in texto:
                fila_consumo = idx
            elif 'hipotecarios para vivienda' in texto:
                fila_hipo = idx
                
    if fila_consumo is None or fila_hipo is None:
        raise ValueError(f"Conceptos no hallados en {ruta_archivo}.")
                
    datos = []
    for col_idx, col_name in enumerate(encabezados):
        if pd.notna(col_name):
            banco = normalizar_nombre_banco(col_name)
            if banco in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Consumo', 'indicador_id': 'Morosidad', 'valor': df.iloc[fila_consumo, col_idx]})
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Hipotecario', 'indicador_id': 'Morosidad', 'valor': df.iloc[fila_hipo, col_idx]})
                
    return pd.DataFrame(datos).drop_duplicates()

def extraer_tasas_activas(ruta_archivo, periodo):
    """Extrae tasas activas en moneda nacional (ventana 30 días)."""
    df = pd.read_excel(ruta_archivo, sheet_name='Reporte', header=None)
    validar_integridad_temporal(df, ruta_archivo, periodo)
    
    encabezados = df.iloc[6]
    fila_consumo, fila_hipo = None, None
    for idx, val in enumerate(df[1]):
        if pd.notna(val):
            texto = str(val).lower().strip()
            if texto == 'consumo':
                fila_consumo = idx
            elif texto == 'hipotecarios':
                fila_hipo = idx
                
    if fila_consumo is None or fila_hipo is None:
        raise ValueError(f"Conceptos de tasas no hallados en {ruta_archivo}.")
    
    datos = []
    for col_idx, col_name in enumerate(encabezados):
        if pd.notna(col_name):
            banco = normalizar_nombre_banco(col_name)
            if banco in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Consumo', 'indicador_id': 'Tasa Activa MN', 'valor': df.iloc[fila_consumo, col_idx]})
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Hipotecario', 'indicador_id': 'Tasa Activa MN', 'valor': df.iloc[fila_hipo, col_idx]})
                
    return pd.DataFrame(datos).drop_duplicates()

def extraer_creditos_directos(ruta_archivo, periodo):
    """Extrae saldos nominales de créditos (B-2334) mediante introspección dinámica de cabeceras."""
    df = pd.read_excel(ruta_archivo, sheet_name=0, header=None)
    validar_integridad_temporal(df, ruta_archivo, periodo)

    cols_consumo = []
    cols_hipo = []
    for c in range(1, df.shape[1]):
        texto_col = " ".join([str(df.iloc[r, c]).lower() for r in range(2, 6) if pd.notna(df.iloc[r, c])])
        if 'consumo' in texto_col:
            cols_consumo.append(c)
        elif 'hipotecario' in texto_col or 'vivienda' in texto_col:
            cols_hipo.append(c)
            
    if not cols_consumo:
        raise ValueError(f"Falla estructural en {ruta_archivo}: No se identificaron columnas para 'Consumo'.")
    if not cols_hipo:
        raise ValueError(f"Falla estructural en {ruta_archivo}: No se identificaron columnas para 'Hipotecario'.")

    datos = []
    for r_idx in range(len(df)):
        banco_val = df.iloc[r_idx, 0]
        if pd.notna(banco_val):
            banco = normalizar_nombre_banco(banco_val)
            if banco in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                cons_tot = pd.to_numeric(df.iloc[r_idx, cols_consumo], errors='coerce').sum()
                hipo_tot = pd.to_numeric(df.iloc[r_idx, cols_hipo], errors='coerce').sum()
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Consumo', 'indicador_id': 'Créditos Directos Totales', 'valor': cons_tot})
                datos.append({'fecha_id': periodo, 'banco_id': banco, 'tipo_credito_id': 'Hipotecario', 'indicador_id': 'Créditos Directos Totales', 'valor': hipo_tot})
                
    return pd.DataFrame(datos).drop_duplicates()

def resolver_archivo_sbs(carpeta, prefijo, mes):
    """Localiza el archivo del periodo admitiendo extensiones .xls, .XLS, .xlsx, .XLSX."""
    for ext in ['.xls', '.XLS', '.xlsx', '.XLSX']:
        ruta = os.path.join(carpeta, f"{prefijo}-{mes}{ext}")
        if os.path.exists(ruta):
            return ruta
    # Búsqueda por glob como fallback
    coincidencias = glob.glob(os.path.join(carpeta, f"{prefijo}-{mes}.*"))
    if coincidencias:
        return coincidencias[0]
    return None

def ejecutar_extraccion_sbs():
    carpeta_raw = "data/raw/SBS"
    anios = [2023, 2024, 2025]
    todos_meses = [f"{a}-{m:02d}" for a in anios for m in range(1, 13)]
    
    print("=" * 65)
    print("INICIANDO EXTRACCIÓN SBS: PERIODO COMPLETO 2023 - 2025 (36 MESES)")
    print("=" * 65)

    dfs_consolidados = []
    archivos_faltantes = []
    errores_extraccion = []

    for mes in todos_meses:
        f_moro = resolver_archivo_sbs(carpeta_raw, "B-2362", mes)
        f_cred = resolver_archivo_sbs(carpeta_raw, "B-2334", mes)
        f_tasa = resolver_archivo_sbs(carpeta_raw, "Tasas", mes)

        # 1. Morosidad (B-2362)
        if f_moro:
            try:
                dfs_consolidados.append(extraer_morosidad(f_moro, mes))
            except Exception as e:
                errores_extraccion.append(f"Error Morosidad {mes} ({f_moro}): {e}")
        else:
            archivos_faltantes.append(f"B-2362-{mes}")

        # 2. Créditos Directos (B-2334)
        if f_cred:
            try:
                dfs_consolidados.append(extraer_creditos_directos(f_cred, mes))
            except Exception as e:
                errores_extraccion.append(f"Error Créditos {mes} ({f_cred}): {e}")
        else:
            archivos_faltantes.append(f"B-2334-{mes}")

        # 3. Tasas Activas
        if f_tasa:
            try:
                dfs_consolidados.append(extraer_tasas_activas(f_tasa, mes))
            except Exception as e:
                errores_extraccion.append(f"Error Tasas {mes} ({f_tasa}): {e}")
        else:
            archivos_faltantes.append(f"Tasas-{mes}")

    if archivos_faltantes:
        print(f"\n⚠️ ARCHIVOS FALTANTES DETECTADOS ({len(archivos_faltantes)}):")
        for f in archivos_faltantes:
            print(f"  - {f}")

    if errores_extraccion:
        print(f"\n❌ ERRORES DE EXTRACCIÓN ({len(errores_extraccion)}):")
        for err in errores_extraccion:
            print(f"  - {err}")

    if not archivos_faltantes and not errores_extraccion:
        df_final = pd.concat(dfs_consolidados, ignore_index=True)
        os.makedirs("data/processed", exist_ok=True)
        ruta_salida = "data/processed/sbs_consolidado_2023_2025.csv"
        df_final.to_csv(ruta_salida, index=False)

        print(f"\n✅ AUDITORÍA Y EXTRACCIÓN EXITOSA: 108/108 ARCHIVOS PROCESADOS")
        print(f"Total de registros generados: {len(df_final)} (Esperado: 1296)")
        print(f"Valores nulos: {df_final.isnull().sum().sum()}")
        print(f"📁 Archivo consolidado guardado en: {ruta_salida}")
        return df_final
    else:
        raise RuntimeError("La extracción no se completó debido a archivos faltantes o errores de parseo.")

if __name__ == "__main__":
    ejecutar_extraccion_sbs()