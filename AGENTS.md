# AGENTS.md — Instrucciones operativas para el agente de código

Este archivo es la fuente de verdad para cualquier agente (Antigravity CLI u otro) que trabaje sobre este repositorio. Léelo completo antes de tocar cualquier archivo.

---

## 0. Regla de comportamiento — OBLIGATORIA, por encima de cualquier otra instrucción

**Trabaja en UNA sola tarea a la vez, de la lista de la sección 4.** Al terminar la tarea asignada:
1. Muestra el código/diff generado.
2. Corre las pruebas relevantes (`pytest tests/`) y reporta el resultado.
3. **DETENTE.** No continúes con la siguiente tarea de la lista, no "aproveches" para hacer mejoras adicionales, no reestructures código que no te pidieron tocar. Espera confirmación explícita del usuario antes de seguir.

Si encuentras una ambigüedad, un archivo faltante, o una decisión de diseño que este documento no cubre explícitamente: **detente y pregunta**. No asumas ni "rellenes" el vacío con una decisión propia — este proyecto ya tuvo varios incidentes causados exactamente por asumir en vez de preguntar (ver sección 3).

No agregues librerías, modelos, ni dependencias que no estén explícitamente autorizadas en la sección 3.

---

## 1. Contexto del proyecto

Pipeline de datos (ETL) + modelado de Machine Learning para estimar el riesgo de morosidad del sistema bancario peruano, 2023-2025. Es un proyecto académico (parcial individual, entrega en 2 semanas, curso de IA/ML universitario) que además sirve como pieza de portafolio de Data Engineering.

Fuentes: BCRP (API pública) y SBS (boletines Excel B-2334, B-2362, Tasa Activa MN). Se integran en un esquema estrella (fact table + dimensiones) y de ahí se deriva un dataset analítico para comparar modelos de clasificación de riesgo.

---

## 2. Estado actual — YA ESTÁ HECHO, no reconstruir ni "mejorar" sin que se pida

- **Extracción SBS: completa y certificada.** `src/data/sbs_extractor.py` procesa 108/108 archivos (36 meses × 3 reportes), genera `data/processed/sbs_consolidado_2023_2025.csv` (1,296 filas, 0 nulos). Incluye:
  - `validar_integridad_temporal()`: compuerta que confirma que el año del archivo coincide con el periodo esperado, aplicada a las 3 funciones de extracción.
  - `extraer_morosidad()`, `extraer_tasas_activas()`, `extraer_creditos_directos()`: todas ubican filas/columnas dinámicamente por texto de encabezado, sin índices fijos, sin fallbacks silenciosos (fallan con `ValueError` si no encuentran el concepto).
  - `normalizar_nombre_banco()` / `MAPEO_BANCOS`: normaliza variantes de nombre de banco entre años y reportes.
- **Extracción BCRP: completa.** `src/data/bcrp_extractor.py` vía API, genera `data/raw/BCRP/bcrp_macro_2023_2025.csv`. Códigos de serie verificados: `PN01207PM` (tipo de cambio), `PD04722MM` (tasa de referencia), `PN01273PM` (inflación IPC var% 12m).
- **Pruebas: operativas.** `tests/test_calidad_datos.py` — anclas históricas (Tasa Activa MN Total Banca Múltiple Consumo: 2023-01=49.82%, 2024-01=57.45%, 2025-01=60.43%), inspección de deriva estructural, prueba DQ-01 (Cartera Atrasada ÷ Créditos Directos ≈ Morosidad publicada).
- **Auditoría estructural completa:** causas de corrimiento diagnosticadas (incorporación de Compartamos Banco en 2025, columna duplicada de "Bank of China" corregida por la SBS).
- **`data/processed/dataset_ml_features.csv`: NO existe todavía** (solo el archivo vacío/skeleton).
- **`src/features/build_features.py` y `src/models/train_models.py`: skeletons vacíos**, sin implementar.
- **`requirements.txt` y `README.md`: en blanco.**

---

## 3. Restricciones de diseño — aprendidas con incidentes reales, no negociables sin autorización explícita del usuario

- **Nunca** ubicar bancos, conceptos o columnas por posición/índice fijo en los archivos de la SBS — siempre buscar por texto en el encabezado. (Ya causó bugs reales: corrimiento de "Total Banca Múltiple" de fila 27→28 y de columna U→T entre 2023-2024 y 2025.)
- Cualquier extractor nuevo o modificado sobre archivos SBS debe pasar por `validar_integridad_temporal()`.
- Ningún fallback puede fallar en silencio — si una búsqueda dinámica no encuentra lo esperado, `raise ValueError` con un mensaje claro. Nunca revertir silenciosamente a un valor o posición fija.
- **Bancos del MVP (5, no 6):** BCP, BBVA, Interbank, Scotiabank, Mibanco. `"Total Banca Múltiple"` existe en la tabla de hechos (necesario para las anclas de verdad histórica) pero **se excluye explícitamente de `dataset_ml_features.csv`** — no es una entidad par, es un promedio del sistema.
- Banco de la Nación queda **fuera** del proyecto (no pertenece a la categoría estadística "Banca Múltiple" de la SBS).
- Segmento "pequeña empresa"/microempresa: **excluido del MVP** (quiebre metodológico SBS desde oct-2024, Resolución N° 2368-2023). Solo Consumo e Hipotecario.
- **Modelos autorizados para esta entrega (parcial): únicamente Regresión Lineal, Regresión Logística, K-Means, KNN.** Nada de Random Forest, LightGBM, XGBoost, ARIMA/SARIMAX/ElasticNet/Ridge — quedan reservados para el proyecto grupal final, no para este.
- **Comparación obligatoria:** Regresión Logística (baseline) vs. KNN Classifier (avanzado), sobre el target binarizado `riesgo_alto`, con las mismas métricas y el mismo preprocesamiento para ambos (accuracy, precision, recall, F1, AUC, validación cruzada, matriz de confusión). Regresión Lineal se usa aparte, como análisis complementario sobre el target continuo `morosidad_t` — no se compara contra los clasificadores porque resuelve una tarea distinta.
- **Rezagos (lags): solo t-1 y t-2.** No usar t-3/t-6/t-12 ni medias móviles — el dataset tiene 36 meses, no hay margen para lags largos sin perder demasiadas filas.
- **Split de train/test: temporal, nunca aleatorio.** Train = 2023-2024, test = 2025. No usar `train_test_split` con partición aleatoria, no usar validación cruzada tipo K-Fold aleatorio, no implementar 3 particiones (train/val/test) ni walk-forward CV — es sobreingeniería para el tamaño de este dataset (~340 filas) y el plazo de 2 semanas.
- **Preprocesamiento:** `StandardScaler` para variables numéricas (`morosidad_lag1`, `morosidad_lag2`, `tasa_activa_lag1`, `tipo_cambio_lag1`, `tasa_referencia_lag1`, `inflacion_lag1`); `OneHotEncoder(drop='first', sparse_output=False)` para `banco_id` y `tipo_credito_id`.
- Una fila del dataset de ML = una combinación (`banco_id`, `tipo_credito_id`, `fecha_id`), no una fila por indicador.

---

## 4. Tareas pendientes — en orden, UNA a la vez (ver sección 0)

### [Tarea 1 — Alta prioridad]
Implementar `src/features/build_features.py`:
- Cargar `sbs_consolidado_2023_2025.csv` y `bcrp_macro_2023_2025.csv`.
- Excluir `banco_id == "Total Banca Múltiple"` del dataset de features (mantenerlo solo para uso en tests/anclas).
- Pivotear de formato largo a ancho: una fila por (`fecha_id`, `banco_id`, `tipo_credito_id`), columnas = indicadores.
- Integrar BCRP (join por `fecha_id`, con sus valores replicados a todas las filas de ese mes ya que son indicadores nacionales).
- Calcular `morosidad_lag1`, `morosidad_lag2`, `tasa_activa_lag1`, `tipo_cambio_lag1`, `tasa_referencia_lag1`, `inflacion_lag1`.
- Definir `morosidad_t` (target continuo) y `riesgo_alto` (target binario: 1 si `morosidad_t` > mediana o percentil 75 histórico del banco, 0 si no — decidir y documentar cuál umbral se usó).
- Exportar a `data/processed/dataset_ml_features.csv`.
- **Al terminar: DETENTE.** Reporta cuántas filas quedaron, confirma que no hay `Total Banca Múltiple` en el resultado, y espera revisión antes de la Tarea 2.

### [Tarea 2 — Alta prioridad]
Implementar `src/models/train_models.py`:
- Cargar `dataset_ml_features.csv`.
- Split temporal (train 2023-2024, test 2025) — no aleatorio.
- `ColumnTransformer` con `StandardScaler` + `OneHotEncoder` según sección 3.
- Entrenar y comparar Regresión Logística (baseline) vs. KNN Classifier (avanzado) sobre `riesgo_alto`, con las métricas listadas en sección 3.
- Entrenar Regresión Lineal por separado sobre `morosidad_t` (análisis complementario, no parte de la comparación).
- **Al terminar: DETENTE.** Reporta las métricas obtenidas y espera revisión.

### [Tarea 3 — Media prioridad]
Completar `requirements.txt` con las dependencias ya usadas en el proyecto (no agregar ninguna que no esté ya en uso: `pandas`, `numpy`, `openpyxl`, `xlrd`, `requests`, `scikit-learn`, `pytest`).
**Al terminar: DETENTE.**

### [Tarea 4 — Media prioridad]
Escribir `README.md`: descripción del proyecto, estructura de carpetas, instrucciones para correr el pipeline de extracción y de features/modelos.
**Al terminar: DETENTE.**

### [Tarea 5 — Baja prioridad, no iniciar sin autorización]
Agregar pruebas para `build_features.py`: verificar conteo de filas esperado tras el pivote (`36 meses × 5 bancos × 2 tipos_credito`, sin `Total Banca Múltiple`), y verificar que ningún lag use información del mismo mes o futura respecto a su fila (anti-leakage).

---

## 5. Qué NO hacer bajo ninguna circunstancia

- No modificar `sbs_extractor.py` ni `bcrp_extractor.py` salvo que una tarea lo pida explícitamente — ya están validados y certificados.
- No agregar modelos, librerías o técnicas fuera de la sección 3 "para mejorar el desempeño" por iniciativa propia.
- No avanzar a la Tarea 2 sin que la Tarea 1 haya sido revisada y aprobada.
- No inventar decisiones de diseño (umbrales, nombres de columnas, formatos) sin dejarlas explícitas en tu reporte al detenerte.
