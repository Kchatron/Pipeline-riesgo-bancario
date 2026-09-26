import os
import json
import urllib.request
import pandas as pd

SERIES_BCRP = {
    'PN01207PM': 'tipo_cambio',
    'PD04722MM': 'tasa_referencia',
    'PN01273PM': 'inflacion'
}

MESES_ES_A_NUM = {
    'Ene': '01', 'Feb': '02', 'Mar': '03', 'Abr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
    'Set': '09', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dic': '12'
}

def mapear_columna_serie(nombre_serie, series_dict):
    """Mapea el nombre o descripción de la serie devuelto por la API a la columna correspondiente."""
    if nombre_serie in series_dict:
        return series_dict[nombre_serie]
    if nombre_serie in series_dict.values():
        return nombre_serie
        
    nombre_lower = str(nombre_serie).lower()
    if 'referencia' in nombre_lower or 'interés' in nombre_lower or 'interes' in nombre_lower:
        return series_dict.get('PD04722MM', 'tasa_referencia')
    elif 'tipo de cambio' in nombre_lower or 'cambio' in nombre_lower:
        return series_dict.get('PN01207PM', 'tipo_cambio')
    elif 'ipc' in nombre_lower or 'inflación' in nombre_lower or 'inflacion' in nombre_lower or 'precios' in nombre_lower:
        return series_dict.get('PN01273PM', 'inflacion')
        
    return nombre_serie

def parsear_fecha_bcrp(nombre_periodo):
    """Convierte el periodo devuelto por BCRP (ej. 'Ene.2023' o 'Sep.2023') a 'YYYY-MM'."""
    limpio = nombre_periodo.strip()
    if '.' in limpio:
        mes_txt, anio_txt = limpio.split('.', 1)
    else:
        mes_txt = limpio[:3]
        anio_txt = limpio[3:]
        
    mes_num = MESES_ES_A_NUM.get(mes_txt.capitalize()[:3]) or MESES_ES_A_NUM.get(mes_txt[:3], '01')
    anio = anio_txt.strip()
    if len(anio) == 2:
        anio = f"20{anio}"
        
    return f"{anio}-{mes_num}"

def extraer_bcrp_api(series_dict=SERIES_BCRP, periodo_ini="2023-1", periodo_fin="2025-12"):
    """Descarga y estandariza las 3 series macroeconómicas del BCRP."""
    codigos = "-".join(series_dict.keys())
    url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{codigos}/json/{periodo_ini}/{periodo_fin}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    
    with urllib.request.urlopen(req) as response:
        payload = json.loads(response.read().decode('utf-8'))
        
    series_meta = [mapear_columna_serie(s['name'], series_dict) for s in payload['config']['series']]
    
    filas = []
    for item in payload['periods']:
        fecha_id = parsear_fecha_bcrp(item['name'])
        fila = {'fecha_id': fecha_id}
        for idx, valor_str in enumerate(item['values']):
            try:
                fila[series_meta[idx]] = float(valor_str)
            except (ValueError, TypeError):
                fila[series_meta[idx]] = None
        filas.append(fila)
        
    df_macro = pd.DataFrame(filas).sort_values('fecha_id').reset_index(drop=True)
    os.makedirs("data/raw/BCRP", exist_ok=True)
    
    # 1. Guardar formato ancho (wide)
    ruta_wide = "data/raw/BCRP/macro_bcrp_2023_2025.csv"
    df_macro.to_csv(ruta_wide, index=False)
    print(f"✅ BCRP: {len(df_macro)} periodos guardados en {ruta_wide}")
    
    # 2. Guardar formato largo armonizado al esquema estrella
    df_long = df_macro.melt(id_vars=['fecha_id'], var_name='indicador_id', value_name='valor')
    df_long['banco_id'] = 'Macro'
    df_long['tipo_credito_id'] = 'Macro'
    df_long = df_long[['fecha_id', 'banco_id', 'tipo_credito_id', 'indicador_id', 'valor']]
    ruta_long = "data/raw/BCRP/bcrp_macro_2023_2025.csv"
    df_long.to_csv(ruta_long, index=False)
    print(f"✅ BCRP (Esquema estrella): {len(df_long)} registros guardados en {ruta_long}")
    
    return df_macro

if __name__ == "__main__":
    extraer_bcrp_api()