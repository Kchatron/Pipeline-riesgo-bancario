import pandas as pd
import os
import glob

# Mapeo estricto para normalizar nombres de bancos entre distintos años/reportes
MAPEO_BANCOS = {
    'b. de crédito del perú': 'BCP',
    'bcp': 'BCP',
    'b. bbva perú': 'BBVA',
    'bbva': 'BBVA',
    'interbank': 'Interbank',
    'scotiabank perú': 'Scotiabank',
    'scotiabank': 'Scotiabank',
    'mibanco': 'Mibanco',
    'total banca múltiple': 'Total Banca Múltiple',
    'promedio': 'Total Banca Múltiple', # Unifica el 'Promedio' de Tasas Activas con el Total del sistema
    'b. de comercio': 'Bancom',
    'bancom': 'Bancom'
}

def normalizar_nombre_banco(nombre_crudo):
    """Convierte el nombre del Excel al nombre estándar del esquema estrella."""
    nombre_limpio = str(nombre_crudo).lower().strip()
    for clave, valor_estandar in MAPEO_BANCOS.items():
        if clave in nombre_limpio:
            return valor_estandar
    return None

def extraer_morosidad(ruta_archivo, periodo):
    """
    Extrae la morosidad buscando dinámicamente columnas y filas, 
    sin depender de posiciones fijas (ej. iloc[:, 18]).
    """
    df = pd.read_excel(ruta_archivo, sheet_name=0, header=None)
    encabezados = df.iloc[5]
    
    # 1. Buscar las filas de Consumo e Hipotecario dinámicamente
    fila_consumo, fila_hipo = None, None
    for idx, val in enumerate(df[0]):
        if pd.notna(val):
            texto = str(val).lower().strip()
            if 'créditos de consumo' in texto: 
                fila_consumo = idx
            elif 'hipotecarios para vivienda' in texto: 
                fila_hipo = idx
                
    datos_extraidos = []
    
    # 2. Buscar las columnas iterando por el nombre, NO por el índice
    for col_idx, col_name in enumerate(encabezados):
        if pd.notna(col_name):
            banco_estandar = normalizar_nombre_banco(col_name)
            
            # Solo extraemos si es uno de los bancos de nuestro MVP
            if banco_estandar in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                val_consumo = df.iloc[fila_consumo, col_idx]
                val_hipo = df.iloc[fila_hipo, col_idx]
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Consumo',
                    'indicador_id': 'Morosidad',
                    'valor': val_consumo
                })
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Hipotecario',
                    'indicador_id': 'Morosidad',
                    'valor': val_hipo
                })
                
    return pd.DataFrame(datos_extraidos)

def extraer_tasas_activas(ruta_archivo, periodo):
    """
    Extrae las tasas activas MN. 
    Resuelve el problema de la columna Promedio que se mueve entre años.
    """
    df = pd.read_excel(ruta_archivo, sheet_name='Reporte', header=None)
    encabezados = df.iloc[6] # Fila 7 en Excel tiene los nombres de los bancos
    
    # En este reporte, las filas de Consumo e Hipotecario son estables
    fila_consumo = 44 # Fila 45 en Excel
    fila_hipo = 51    # Fila 52 en Excel
    
    datos_extraidos = []
    
    for col_idx, col_name in enumerate(encabezados):
        if pd.notna(col_name):
            banco_estandar = normalizar_nombre_banco(col_name)
            
            # Filtramos solo los bancos del MVP
            if banco_estandar in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                val_consumo = df.iloc[fila_consumo, col_idx]
                val_hipo = df.iloc[fila_hipo, col_idx]
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Consumo',
                    'indicador_id': 'Tasa Activa MN',
                    'valor': val_consumo
                })
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Hipotecario',
                    'indicador_id': 'Tasa Activa MN',
                    'valor': val_hipo
                })
                
    # Eliminamos duplicados por si acaso el banco apareciera dos veces (como pasó con Bank of China)
    return pd.DataFrame(datos_extraidos).drop_duplicates()

def extraer_creditos_directos(ruta_archivo, periodo):
    """
    Extrae los saldos de créditos directos (B-2334).
    Aquí la matriz está traspuesta: los bancos están en las FILAS.
    """
    df = pd.read_excel(ruta_archivo, sheet_name=0, header=None)
    datos_extraidos = []
    
    # Iteramos sobre las filas buscando los bancos en la columna 0
    for r_idx in range(len(df)):
        banco_val = df.iloc[r_idx, 0]
        if pd.notna(banco_val):
            banco_estandar = normalizar_nombre_banco(banco_val)
            
            if banco_estandar in ['BCP', 'BBVA', 'Interbank', 'Scotiabank', 'Mibanco', 'Total Banca Múltiple']:
                # Consumo (suma de las columnas 21 a la 26 inclusive)
                cons_tot = pd.to_numeric(df.iloc[r_idx, 21:27], errors='coerce').sum()
                
                # Hipotecario (suma de las columnas 28, 29 y 30)
                hipo_tot = pd.to_numeric(df.iloc[r_idx, 28:31], errors='coerce').sum()
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Consumo',
                    'indicador_id': 'Créditos Directos Totales',
                    'valor': cons_tot
                })
                
                datos_extraidos.append({
                    'fecha_id': periodo,
                    'banco_id': banco_estandar,
                    'tipo_credito_id': 'Hipotecario',
                    'indicador_id': 'Créditos Directos Totales',
                    'valor': hipo_tot
                })
                
    return pd.DataFrame(datos_extraidos)

if __name__ == "__main__":
    # Nombres de tus archivos de enero 2023
    ruta_morosidad = "data/raw/SBS/B-2362-en2023.XLS"
    ruta_tasas = "data/raw/SBS/B_TIActivaTipoCreditoEmpresaMN105021_2023.xlsx"
    ruta_creditos = "data/raw/SBS/B-2334-en2023.XLS"
    
    periodo_prueba = "2023-01"
    
    print("--- 1. PROBANDO MOROSIDAD ---")
    if os.path.exists(ruta_morosidad):
        print(extraer_morosidad(ruta_morosidad, periodo_prueba).head())
    
    print("\n--- 2. PROBANDO TASAS ACTIVAS ---")
    if os.path.exists(ruta_tasas):
        print(extraer_tasas_activas(ruta_tasas, periodo_prueba).head())
        
    print("\n--- 3. PROBANDO CRÉDITOS DIRECTOS ---")
    if os.path.exists(ruta_creditos):
        print(extraer_creditos_directos(ruta_creditos, periodo_prueba).head())