# Pipeline de Datos y Estimación de Riesgo de Morosidad Bancaria en el Perú (2023–2025)

Pipeline integral de **Ingeniería de Datos (ETL)** y **Machine Learning Predictivo** diseñado para monitorear, integrar y proyectar el riesgo crediticio y la morosidad en el sistema bancario peruano para el periodo mensual 2023–2025. 

Integra indicadores macroeconómicos provistos por el **Banco Central de Reserva del Perú (BCRP)** con boletines financieros de la **Superintendencia de Banca, Seguros y AFP (SBS)** en un modelo dimensional (esquema estrella), derivando un dataset analítico con rezagos temporales (*lags*) libres de fuga de datos (*data leakage*) para comparar modelos de clasificación y regresión.

---

## 1. Alcance del Proyecto

- **Entidades Financieras (5 Bancos MVP):** Banco de Crédito del Perú (BCP), BBVA, Interbank, Scotiabank y Mibanco.
  > *Nota de diseño:* El agregado `"Total Banca Múltiple"` se conserva en el consolidado dimensional para validación de anclas históricas y agregados del sistema, pero se excluye explícitamente del dataset de Machine Learning para evitar sesgos de encuadre en las entidades individuales.
- **Ventana Temporal:** Enero 2023 a Diciembre 2025 (36 meses continuos, frecuencia mensual).
- **Segmentos Crediticios:** Créditos de Consumo y Créditos Hipotecarios (segmentos estables y metodológicamente homogéneos según la normativa SBS).
- **Fuentes de Información:**
  1. **BCRP (API pública JSON):**
     - Tipo de cambio interbancario PEN/USD (`PN01207PM`).
     - Tasa de interés de referencia de política monetaria (`PD04722MM`).
     - Inflación IPC var% anual 12 meses (`PN01273PM`).
  2. **SBS (Boletines Estadísticos Excel):**
     - Boletín B-2362: Índices de Morosidad por tipo de crédito.
     - Boletín B-2334: Créditos Directos y Cartera Atrasada.
     - Reporte Tasa Activa Promedio MN por tipo de crédito y empresa.

---

## 2. Estructura del Repositorio

```text
├── Graficas_Comparativas_Modelos/           # Gráficos diagnósticos y matrices de confusión (PNG 300 DPI)
│   ├── 01_comparativa_metricas_clasificacion.png
│   ├── 02_matrices_confusion.png
│   ├── 03_cuadro_comparativo_resumen.png
│   ├── 04_regresion_lineal_pred_vs_real.png
│   └── 05_coeficientes_regresion_lineal.png
├── data/
│   ├── processed/
│   │   ├── dataset_ml_features.csv          # Dataset analítico final (340 filas, 23 columnas)
│   │   ├── ejemplo_decisiones_test_2025.csv # Artefacto de triaje operativo de riesgo (120 filas)
│   │   └── sbs_consolidado_2023_2025.csv    # Consolidado SBS armonizado (1,296 registros)
│   └── raw/
│       ├── BCRP/                            # Series macroeconómicas crudas (CSV wide y long)
│       └── SBS/                             # 108 reportes Excel oficiales (36 meses × 3 reportes)
├── notebooks/                               # Análisis exploratorio y prototipado
├── src/
│   ├── data/
│   │   ├── bcrp_extractor.py                # Extractor de series macroeconómicas vía API BCRP
│   │   └── sbs_extractor.py                 # Extractor dinámico y compuertas de integridad SBS
│   ├── features/
│   │   └── build_features.py                # Transformación ancho/largo, rezagos y definición de targets
│   └── models/
│       ├── decision_rules.py                # Capa de reglas de negocio operativas sobre probabilidades
│       └── train_models.py                  # Modelado out-of-time, evaluación y generación de gráficas
├── tests/
│   ├── test_calidad_datos.py                # Pruebas automatizadas de calidad (DQ) y anclas históricas
│   └── test_features.py                     # Validación de anti-leakage y cálculo de umbral en Train
├── diseno-pipeline-riesgo-bancario-pe.md    # Especificación de arquitectura y modelo de datos
├── Informe_Parcial_Pipeline_Riesgo_Bancario.docx # Informe técnico formal con resultados y gráficas
├── requirements.txt                         # Dependencias exactas del proyecto
└── README.md                                # Documentación principal
```

---

## 3. Instalación y Requisitos

El proyecto requiere **Python 3.10+**. Se recomienda utilizar un entorno virtual dedicado:

```bash
# 1. Clonar el repositorio
git clone <URL_DEL_REPOSITORIO>
cd "Proyecto Inteligencia Artificial"

# 2. Crear y activar el entorno virtual
python3 -m venv .venv
source .venv/bin/activate   # En Linux / macOS
# .venv\Scripts\activate    # En Windows

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## 4. Ejecución del Pipeline Paso a Paso

El pipeline se encuentra modularizado para ser ejecutado de forma secuencial y reproducible:

### Paso 1: Extracción de Datos Crudos

Descarga y estandariza los datos macroeconómicos del BCRP y procesa los 108 reportes de la SBS aplicando detección dinámica de encabezados:

```bash
# Extracción macroeconómica (API BCRP)
python src/data/bcrp_extractor.py

# Extracción y armonización de boletines financieros SBS
python src/data/sbs_extractor.py
```

### Paso 2: Validación de Calidad de Datos y Anti-Leakage (Data Quality)

Ejecuta la suite completa de pruebas unitarias automatizadas (`test_calidad_datos.py` y `test_features.py`):
1. Verifica que la tabla de hechos contenga exactamente 1,296 observaciones con 0 valores nulos.
2. Valida contra anclas de verdad histórica oficial de la SBS (Tasa Activa Consumo Total Banca Múltiple: Ene-23 = 49.82%, Ene-24 = 57.45%, Ene-25 = 60.43%) y consistencia contable (regla DQ-01).
3. Certifica que la matriz analítica tenga 340 filas, que no contenga a "Total Banca Múltiple", que los rezagos ($t-1, t-2$) no sufran fuga temporal y que el percentil 75 se calcule exclusivamente sobre el conjunto de entrenamiento:

```bash
pytest tests/
```

### Paso 3: Feature Engineering y Rezagos Temporales

Pivotea la información bancaria a formato ancho, integra los factores macroeconómicos y construye rezagos temporales ($t-1$, $t-2$) por panel bancario-cartera para eliminar el *data leakage*. Define el target continuo `morosidad_t` y el target binario `riesgo_alto` (percentil 75 histórico por banco y cartera):

```bash
python src/features/build_features.py
```
*Salida:* `data/processed/dataset_ml_features.csv` (340 filas útiles sin nulos, 5 bancos × 2 carteras × 34 meses).

### Paso 4: Entrenamiento, Evaluación, Gráficas y Capa de Decisión

Aplica un particionamiento temporal estricto (*out-of-time*), preprocesa con `ColumnTransformer` (`StandardScaler` + `OneHotEncoder`), entrena los modelos, exporta las métricas y gráficos diagnósticos, y aplica la capa de reglas de negocio operativas:

```bash
python src/models/train_models.py
```
*Salida de decisión:* `data/processed/ejemplo_decisiones_test_2025.csv` (120 expedientes clasificados).

---

## 5. Estrategia de Modelado y Resultados

### 5.1 Enfoque de Partición Temporal (Out-of-Time)

A diferencia de los problemas transversales (*cross-sectional*), este proyecto maneja datos de panel temporal. Para evitar sobreoptimismo y evaluar la verdadera capacidad predictiva ante cambios de régimen económico, no se utiliza partición aleatoria:
- **Entrenamiento (Train):** Marzo 2023 a Diciembre 2024 (22 meses × 10 series = 220 observaciones, 64.7%).
- **Evaluación (Test):** Enero 2025 a Diciembre 2025 (12 meses × 10 series = 120 observaciones, 35.3%).

### 5.2 Comparación de Modelos de Clasificación (`riesgo_alto`)

Se evalúa la capacidad de anticipar si la morosidad de una entidad superará su umbral histórico crítico ($Q_3$ o percentil 75):

| Modelo | Enfoque | Accuracy (Test) | Precision (Test, Clase 1) | Recall (Test, Clase 1) | F1-Score (Test) | ROC-AUC (Test) |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Regresión Logística** | Baseline | 64.17% | 40.62% | **83.87%** | 0.5474 | **0.8427** |
| **KNN Classifier ($k=5$)** | Avanzado | **72.50%** | **48.08%** | 80.65% | **0.6024** | 0.8373 |

*Hallazgo clave:* Al derivar el percentil 75 estrictamente del periodo de entrenamiento (2023–2024), se eliminó la fuga de la frontera de decisión hacia el año 2025. Ambos clasificadores logran una sólida capacidad de discriminación en test fuera de muestra, con un **ROC-AUC superior a 0.83** y una sensibilidad (*Recall*) que supera el 80% en la detección oportuna de regímenes de estrés crediticio en 2025. KNN ($k=5$) alcanza un mayor equilibrio global (Accuracy 72.50%, F1 0.6024).

### 5.3 Análisis Complementario: Regresión Lineal (`morosidad_t`)

Se modela la tasa continua de morosidad para cuantificar la persistencia y la sensibilidad macroeconómica:
- **$R^2$ Score (Test 2025):** **0.9920** (Train: 0.9712)
- **Error Absoluto Medio (MAE Test):** **0.1066 pp**
- **Raíz del Error Cuadrático Medio (RMSE Test):** **0.1449 pp**
- **Factores Clave:**
  - `morosidad_lag1` (+1.0515): Dominancia autorregresiva de la cartera crediticia.
  - `banco_id_Mibanco` (+0.1435): Mayor prima de riesgo estructural en microfinanzas.
  - `tasa_referencia_lag1` (+0.0710): Mayor costo del dinero presiona la morosidad futura.
  - `tipo_credito_id_Hipotecario` (-0.1362): Cartera hipotecaria estructuralmente más resiliente que consumo.

### 5.4 Capa de Decisión Operativa (Reglas de Negocio en Test 2025)

Las probabilidades calibradas de la Regresión Logística (modelo óptimo para alerta temprana por su Recall de 83.87%) se transforman en acciones operativas de gestión crediticia para las 120 observaciones del año de prueba 2025:

| Nivel de Riesgo | Banda de Probabilidad | Casos Test 2025 (%) | Acción Operativa Recomendada |
|:---:|:---:|:---:|:---|
| **Bajo** | $P < 0.35$ | 18 (15.0%) | Aprobación automática / Monitoreo estándar |
| **Medio** | $0.35 \le P < 0.65$ | 65 (54.2%) | Revisión manual / Ajuste de tasa y mitigantes |
| **Alto** | $P \ge 0.65$ | 37 (30.8%) | Rechazo preventivo / Auditoría estricta de cartera |

---

## 6. Visualizaciones Generadas

Las gráficas de evaluación se exportan automáticamente en `Graficas_Comparativas_Modelos/`:
- `01_comparativa_metricas_clasificacion.png`: Comparativa de barras agrupadas (Accuracy, Precision, Recall, F1, AUC).
- `02_matrices_confusion.png`: Matrices de confusión con conteos y porcentajes para Test 2025.
- `03_cuadro_comparativo_resumen.png`: Tabla gráfica comparativa lista para informes ejecutivos.
- `04_regresion_lineal_pred_vs_real.png`: Dispersión de morosidad observada vs predicha con banda de identidad ($y = x$).
- `05_coeficientes_regresion_lineal.png`: Diagrama de impacto de factores explicativos.

---

## 7. Principios y Calidad de Código

- **Resiliencia ante corrimientos estructurales:** Todos los extractores SBS buscan conceptos y columnas dinámicamente por patrones textuales en encabezados, sin asumir posiciones o índices de celda fijos.
- **Fail-Fast (Sin fallbacks silenciosos):** Cualquier inconsistencia estructural o período desalineado interrumpe la ejecución mediante excepciones explícitas (`ValueError`).
- **Garantía Anti-Leakage:** Los rezagos se calculan estrictamente dentro de cada panel (`banco_id`, `tipo_credito_id`), asegurando que ninguna estimación en el mes $t$ tenga acceso a información contemporánea o futura.
