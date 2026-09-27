"""
decision_rules.py — Capa de Decisión Simple

Transforma las probabilidades predictivas de riesgo_alto
en reglas de negocio operativas (Prioridad / Acción / Revisión).
"""

import pandas as pd
import numpy as np

def aplicar_capa_decision(probs, umbral_bajo=0.3, umbral_alto=0.7):
    """
    Aplica reglas de negocio sobre las probabilidades calibradas.
    
    Categorías:
    - prob < umbral_bajo: Riesgo Bajo -> Acción: Aprobación / Monitoreo Estándar
    - umbral_bajo <= prob < umbral_alto: Riesgo Medio -> Acción: Revisión Manual / Ajuste de Tasa
    - prob >= umbral_alto: Riesgo Alto -> Acción: Rechazo Preventivo / Auditoría Estricta
    """
    decisiones = []
    
    for p in probs:
        if p < umbral_bajo:
            decisiones.append({
                'Probabilidad': p,
                'Nivel_Riesgo': 'Bajo',
                'Prioridad': 'Baja',
                'Accion_Recomendada': 'Aprobación / Monitoreo estándar'
            })
        elif p < umbral_alto:
            decisiones.append({
                'Probabilidad': p,
                'Nivel_Riesgo': 'Medio',
                'Prioridad': 'Media',
                'Accion_Recomendada': 'Revisión manual / Ajuste de tasa'
            })
        else:
            decisiones.append({
                'Probabilidad': p,
                'Nivel_Riesgo': 'Alto',
                'Prioridad': 'Alta',
                'Accion_Recomendada': 'Rechazo preventivo / Auditoría estricta'
            })
            
    return pd.DataFrame(decisiones)

def generar_resumen_decisiones(df_decisiones):
    """Genera un resumen estadístico de las decisiones tomadas."""
    resumen = df_decisiones['Nivel_Riesgo'].value_counts().reset_index()
    resumen.columns = ['Nivel de Riesgo', 'Cantidad de Casos']
    resumen['Porcentaje'] = (resumen['Cantidad de Casos'] / len(df_decisiones) * 100).round(1).astype(str) + '%'
    return resumen
