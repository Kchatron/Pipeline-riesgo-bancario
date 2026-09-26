# Documento de Diseño — Pipeline de Riesgo Financiero del Sistema Bancario Peruano
### (con componente predictivo de morosidad)

**Estado:** v3 — auditoría de fuentes cerrada, listo para extracción de 36 meses
**Fecha de inicio:** 08 de setiembre de 2026
**Duración estimada:** 4-5 semanas (parcial: 2 semanas / final grupal: 6 semanas)
**Autor:** Christian Ore Huilcara

---

## 0. Estado actual del proyecto

### ✅ Completado
- Alcance cerrado: 5 bancos (BCP, BBVA, Interbank, Scotiabank, Mibanco), periodo enero 2023 – diciembre 2025, frecuencia mensual.
- Fuentes verificadas: BCRP (API, 3 códigos confirmados: `PN01207PM` tipo de cambio, `PD04722MM` tasa de referencia, `PN01273PM` inflación IPC) y SBS (B-2334, B-2362, Tasa Activa MN).
- **Extracción SBS completa y certificada:** 108/108 archivos procesados, 1,296/1,296 registros, 0 nulos, anclas históricas validadas contra el CSV final (2023/2024/2025 exactos). `sbs_extractor.py` con compuerta de integridad temporal (`validar_integridad_temporal`) aplicada uniformemente a las 3 funciones de extracción, y sin fallbacks silenciosos a posiciones fijas.
- Auditoría estructural completa sobre las 3 fuentes SBS, causas de corrimiento diagnosticadas con código real (Compartamos Banco, columna duplicada Bank of China).
- Archivo maestro: `data/processed/sbs_consolidado_2023_2025.csv`.
- Esquema estrella v2 diseñado (con `dim_tipo_credito`, `dim_indicador`, patrón `"Macro"` explícito en vez de NULL).
- Regla de calidad DQ-01 validada en múltiples periodos y bancos (BCP, BBVA).
- **Plan de modelado corregido tras revisar cuaderno 11 (ver sección 8):** la comparación baseline-vs-avanzado debe hacerse entre modelos de la MISMA tarea. Regresión Lineal y Regresión Logística resuelven tareas distintas (continua vs. binaria) y no son comparables entre sí — se separan sus roles.
- Estrategia de reutilización parcial → final → portafolio definida (sección 1.1).

### ⚠️ Pendiente antes de generalizar a los 36 meses
- Verificar visualmente en el Excel real si "Consumo"/"Hipotecarios" son el total del segmento o una subcategoría (pendiente de una confirmación explícita, aunque el patrón de negrita/sangría ya lo sugiere).
- ✅ Asimetría temporal — investigada, sin solución de código con el reporte actual; documentada como limitación estructural. Mitigación reservada para la fase grupal.
- Decidir tratamiento del segmento "pequeña empresa" (quiebre metodológico SBS oct-2024) — recomendado: excluir del MVP inicial.
- Confirmar con el profesor qué secciones de las 29 del informe aplican al parcial de 2 semanas.
- **Nueva decisión pendiente:** ¿`banco_id` entra como variable categórica (OneHotEncoder) para que el modelo aprenda efectos por banco, o el modelo se entrena solo con variables macro/rezagadas, tratando el panel de forma agrupada? Definir antes de construir el `ColumnTransformer`.

### ▶️ Próximos pasos (en orden)
1. Cerrar los pendientes de arriba.
2. Integrar BCRP al dataset consolidado (join por `fecha_id`, `banco_id="Macro"`).
3. Pivotear de formato largo a ancho (una columna por indicador) y calcular rezagos (t-1, t-2).
4. Cargar el esquema estrella a RDS PostgreSQL, aplicando calidad de datos (sección 9).
5. Split temporal (train 2023-2024 / test 2025) — **no usar `train_test_split` aleatorio**, el dataset es un panel temporal.
6. Construir `ColumnTransformer` (StandardScaler + OneHotEncoder si corresponde) y entrenar: Regresión Logística (baseline) vs. KNN Classifier (avanzado) sobre el target binarizado `riesgo_alto`, siguiendo la estructura del cuaderno 11 (mismas métricas: accuracy, precision, recall, F1, AUC, validación cruzada). Regresión Lineal queda como análisis complementario para la pregunta de negocio de Nivel 3, no como parte de la comparación obligatoria.
7. Documentar y preparar el informe del parcial.

## 1. Resumen y objetivo



Construir un pipeline de datos (ETL/ELT) que integre indicadores macroeconómicos del BCRP e indicadores financieros de la SBS en un **esquema estrella**, y a partir de ese modelo generar un dataset analítico para entrenar modelos de Machine Learning que **estimen la morosidad bancaria del siguiente periodo**.

El proyecto tiene dos caras deliberadas:
- **Ingeniería de Datos**: extracción → almacenamiento raw → transformación → calidad → modelo dimensional → PostgreSQL/RDS.
- **IA/ML**: usar el dataset construido para predecir morosidad, cumpliendo el requisito del curso sin convertir la extracción en un ejercicio decorativo.

### 1.1 Estrategia: parcial → final → portafolio

- **Parcial (2 semanas, individual):** versión con Regresión Lineal + Regresión Logística (los modelos permitidos por el profesor), cubriendo las fases iniciales de CRISP-DM (comprensión del problema, comprensión y preparación de datos, primer modelo).
- **Final (6 semanas, grupal):** el mismo proyecto, madurado — ciclo CRISP-DM completo, posible incorporación de Random Forest u otro modelo "avanzado" (confirmar con el profesor si el grupo puede salir del listado de 4 modelos), prototipo funcional, informe completo de 29 secciones.
- **Portafolio/CV:** este proyecto es la prioridad real. La restricción académica de modelos no limita la versión de portafolio — después de la entrega, se puede extender independientemente (mejores modelos, más feature engineering) como iteración personal. Encuadrar el README como proyecto propio que además cumplió un requisito de curso, no al revés.

## 2. Alcance

- **Bancos incluidos (decisión final):** BCP, BBVA, Interbank, Scotiabank, Mibanco. Banco de la Nación excluido — no pertenece a la categoría estadística "Banca Múltiple" de la SBS (aparece como línea separada) y su cartera es casi enteramente consumo/hipotecario, sin actividad comparable en otros segmentos.
- **Periodo:** enero 2023 – diciembre 2025 (36 meses × 5 bancos = 180 observaciones por indicador bancario, antes de desagregar por tipo de crédito).
- **Frecuencia:** mensual. Donde la fuente sea diaria, se agrega a mensual en la transformación con una regla definida.
- **Indicadores MVP:**
  - BCRP: tasa de referencia, tipo de cambio PEN/USD, inflación (IPC)
  - SBS: morosidad (Total, Consumo, Hipotecario), tasas activas MN (Total, Consumo, Hipotecario), créditos directos (para contextualizar tamaño de cartera)
  - Excluido del MVP inicial: segmento "pequeña empresa"/microempresa (quiebre metodológico SBS desde oct-2024 — ver Resolución SBS N° 2368-2023). Se puede agregar como extensión si el tiempo alcanza, documentando el quiebre con una variable `post_reforma_sbs`.

## 3. Preguntas de negocio (tres niveles)

- **Nivel 1 — Exploración:** ¿Cómo evoluciona la morosidad de cada banco durante 2023-2025?
- **Nivel 2 — Relación:** ¿Qué relación existe entre la morosidad bancaria y variables macroeconómicas (tipo de cambio, inflación, tasa de referencia)?
- **Nivel 3 — Predicción (ML):** ¿Es posible predecir/clasificar el riesgo de morosidad del siguiente mes usando indicadores macro y financieros históricos?

Cada nivel valida el anterior: no se modela (nivel 3) sin haber confirmado que existe relación (nivel 2).

## 4. Fuentes de datos

### 4.1 BCRP (API pública, sin autenticación)
- `https://estadisticas.bcrp.gob.pe/estadisticas/series/api/[códigos]/[formato]/[periodo_inicial]/[periodo_final]`
- Verificado: `.../api/PN01207PM/json` → Tipo de cambio interbancario promedio, mensual.
- **Pendiente:** confirmar códigos de tasa de referencia e inflación en el buscador de BCRPData.

### 4.2 SBS (sin API JSON — boletines Excel)
- B-2362 (Morosidad), B-2334 (Créditos Directos), Tasa Activa MN por Tipo de Crédito y Empresa.
- Formato: Excel por boletín → `pandas.read_excel()`.

### 4.3 Auditoría estructural — CERRADA sobre la muestra de 3 meses (enero 2023/2024/2025)

Se ejecutó código real sobre los 3 reportes SBS para los 3 años. Tres causas de corrimiento fueron diagnosticadas, cada una distinta — no existe una única causa universal:

| Reporte | Qué se movió | Causa real confirmada |
|---|---|---|
| B-2334 (Créditos Directos) | TOTAL BANCA MÚLTIPLE: fila 27 → 28 (2025) | Compartamos Banco se incorporó como nueva entidad (conversión de financiera a banco, Resolución SBS N° 00348-2025) |
| B-2362 (Morosidad) | TOTAL BANCA MÚLTIPLE: columna S → T (2025) | Mismo efecto de Compartamos, pero horizontal |
| Tasa Activa MN | Promedio: columna U → T (2025) | SBS eliminó una columna duplicada de "Bank of China" que existía por error en 2023-2024 (no relacionado a Compartamos — este reporte corta al 09/01, antes de la resolución del 30/01) |

Las 5 entidades del MVP (BCP, BBVA, Interbank, Scotiabank, Mibanco) mantuvieron posición estable en las 3 fuentes durante los 3 años — bajo riesgo, pero la extracción debe seguir basada en nombre, no en esa estabilidad.

**Reglas obligatorias del extractor:**
1. Nunca ubicar entidades por posición fija (`df.iloc[:, 3]`) — buscar por nombre en el encabezado.
2. Nunca ubicar el total por posición fija — buscar por nombre exacto (`"TOTAL BANCA MÚLTIPLE"` en B-2334/B-2362, `"Promedio"` en Tasa Activa — etiquetas distintas, no asumir que son la misma).
3. Normalizar nombres de banco con un mapeo explícito (ej. `"B. de Comercio"` en 2023 → `"BANCOM"` desde 2024, si se decide incluir ese banco).
4. No eliminar columnas duplicadas automáticamente — comparar valores primero; si son idénticas, descartar una; si difieren, detener y revisar.

**Pendiente antes de los 36 meses (cerrar esto, no más auditoría después):**
- Asimetría temporal: no tiene solución de código con el reporte actual — se documenta como limitación estructural conocida. Mitigación disponible si hay tiempo: reasignar la observación al mes donde cae la mayor parte de la ventana de 30 días.
- **⚠️ Bug encontrado en `sbs_extractor.py` (bloqueante antes de escalar a 36 meses):** `extraer_tasas_activas()` busca las columnas de banco dinámicamente (correcto), pero usa `fila_consumo = 44` y `fila_hipo = 51` como posiciones **fijas** para las filas — exactamente lo que la regla 1 del extractor prohíbe. Ya se demostró que las posiciones (fila o columna) pueden moverse de un año a otro (Compartamos, Bank of China); no hay garantía de que 44/51 se mantengan estables en los 36 meses. Corregir buscando "Consumo"/"Hipotecario" por texto en la columna de conceptos, igual que se hizo con los bancos.
- Verificar visualmente (no en texto plano) si "Consumo"/"Hipotecarios" son el total del segmento o una subcategoría — la indentación en el texto extraído es ambigua.

## 5. Modelo de datos (esquema estrella)

**Regla de scope entre la tabla de hechos y el dataset de ML:** `"Total Banca Múltiple"` permanece en `fact_indicador_financiero` como agregado del sistema — es necesario para las anclas de verdad histórica (tests/test_calidad_datos.py) y para preguntas de negocio de Nivel 1. **Pero se excluye explícitamente de `dataset_ml_features.csv`** — no es una entidad par de BCP/BBVA/Interbank/Scotiabank/Mibanco, es un promedio, y si entra como una "sexta categoría" en el `OneHotEncoder(banco_id)`, contamina tanto el encoding como la interpretación de los efectos fijos por banco.

**Grano de la tabla de hechos:** un valor de un indicador, para un banco (o "Macro" si es nacional), para un tipo de crédito (si aplica), en una fecha específica.

```
fact_indicador_financiero
├── fact_id            (PK)
├── banco_id           (FK → dim_banco; usa el miembro explícito "Macro" para indicadores nacionales, no NULL)
├── fecha_id           (FK → dim_fecha)
├── indicador_id       (FK → dim_indicador)
├── tipo_credito_id    (FK → dim_tipo_credito; usa "Macro" si no aplica)
└── valor              (DECIMAL)

dim_banco
├── banco_id  (PK)
└── nombre_banco        (incluye el miembro "Macro" para indicadores nacionales del BCRP)

dim_fecha
├── fecha_id  (PK)
├── fecha, mes, trimestre, anio

dim_indicador
├── indicador_id  (PK)
├── nombre_indicador
├── unidad
└── fuente          (BCRP / SBS)

dim_tipo_credito
├── tipo_credito_id  (PK)
└── nombre_tipo_credito   (Total, Consumo, Hipotecario; incluye "Macro" si no aplica)
```

**Regla de unicidad:** la combinación `banco + fecha + indicador + tipo_credito` debe identificar un único valor — implementar como `UNIQUE` en PostgreSQL para detectar errores del propio ETL.

## 6. Diccionario de datos (completar conforme se extrae)

| Columna | Tabla | Tipo | Fuente | Descripción |
|---|---|---|---|---|
| valor | fact_indicador_financiero | DECIMAL(12,4) | BCRP/SBS | Valor del indicador en su unidad |
| nombre_banco | dim_banco | VARCHAR(100) | SBS | Nombre comercial normalizado |
| nombre_indicador | dim_indicador | VARCHAR(100) | — | Nombre estandarizado entre fuentes |
| nombre_tipo_credito | dim_tipo_credito | VARCHAR(50) | SBS | Segmento de crédito |
| *(completar conforme se avanza)* | | | | |

## 7. Arquitectura técnica

```
[BCRP API] ──┐
             ├──> [S3: raw/] ──> [Python/pandas: transformación + calidad] ──> [RDS PostgreSQL: esquema estrella]
[SBS Excel]──┘                                                                        │
                                                                     ┌──────────────────┴──────────────────┐
                                                                     ▼                                     ▼
                                                              Consultas SQL                          Dataset ML (features con rezago)
                                                                                                             │
                                                                                                             ▼
                                                                                        Regresión Lineal (baseline) + Regresión Logística
```

- **S3**: capa raw, exactamente lo descargado, sin transformar.
- **RDS PostgreSQL** (`db.t3.micro`/`db.t4g.micro`, free tier): esquema estrella final.
- Detener la instancia RDS cuando no se use activamente — no dejarla corriendo "por si acaso".

## 8. Componente de Machine Learning

- **Target continuo:** `morosidad_t` (por banco, por mes).
- **Target binarizado (para la comparación de clasificación):** `riesgo_alto` = 1 si `morosidad_t` supera la mediana/percentil 75 histórico del banco, 0 si no.
- **Features (con rezago temporal — obligatorio):** `morosidad_t-1`, `morosidad_t-2`, `inflacion_t-1`, `tipo_cambio_t-1`, `tasa_referencia_t-1`, `tasa_activa_t-1`.
  El rezago es obligatorio: usar `inflación_enero → morosidad_enero` es correlación contemporánea, no predicción — mismo principio de *data leakage* visto en el cuaderno 4 del curso.
- **Split:** temporal, no aleatorio (**no usar `train_test_split` con partición aleatoria**, como sí es válido en los datasets cross-sectional de los cuadernos 10/11). **Train = 2023-2024, test = 2025**, global (no por banco). Proporción real ≈ 67/33, no 80/20 — el criterio es dejar un año completo fuera del entrenamiento, no cumplir un porcentaje.

- **Corrección tras revisar el cuaderno 11:** la comparación baseline-vs-avanzado exigida por el profesor debe hacerse entre modelos que resuelven la MISMA tarea (mismo target, mismas métricas) — igual que el cuaderno 11 compara 4 clasificadores entre sí sobre el mismo split y el mismo problema. Regresión Lineal (target continuo) y Regresión Logística (target binario) no son comparables entre sí bajo ese criterio. Se separan los roles:
  - **Comparación obligatoria del parcial (clasificación, sobre `riesgo_alto`):** Regresión Logística (baseline) vs. **KNN Classifier** (avanzado) — misma estructura que el cuaderno 11: accuracy, precision, recall, F1-score, AUC, validación cruzada, matriz de confusión.
  - **Análisis complementario (regresión, sobre `morosidad_t` continua):** Regresión Lineal, para responder la pregunta de negocio de Nivel 3 (sección 3). No forma parte de la comparación obligatoria, pero aporta valor de negocio adicional.
- **Decisión pendiente antes de construir el `ColumnTransformer`:** ¿incluir `banco_id` como variable categórica (`OneHotEncoder`) para capturar efectos específicos por banco, o entrenar solo con variables macro/rezagadas de forma agrupada? El `StandardScaler` sí es necesario en cualquier caso — a diferencia del dataset de laptops del cuaderno 10 (donde era más una buena práctica que una necesidad, por usar Random Forest), aquí las features tienen escalas muy distintas (tasa activa ~50-60 vs. inflación ~1-3 vs. tipo de cambio ~3.7), lo que sí afecta a Regresión Logística y KNN.
- **Modelos candidatos para el final grupal:** Random Forest u otro modelo "avanzado" adicional (confirmar con el profesor si el grupo puede salir del listado de 4 modelos presentados). K-means disponible como análisis exploratorio complementario (segmentar banco-mes por perfil de riesgo), no como modelo predictivo.
- **Honestidad académica a declarar explícitamente:** el dataset final tendrá ~180 filas (5 bancos × 36 meses). Apropiado para un ejercicio educativo/baseline, no para un modelo de producción. El valor del proyecto está en el pipeline y el rigor metodológico, no en la precisión final del modelo.

## 9. Calidad de datos

**DQ-01 — Consistencia de morosidad (control, no forma parte del esquema estrella):** `Cartera Atrasada ÷ Créditos Directos × 100` debe aproximarse a la Morosidad publicada por la SBS. Ya validado para BCP Consumo 2024/2025. Cartera Atrasada se usa solo durante la auditoría, no entra al modelo dimensional.

Validaciones adicionales antes de cargar al modelo dimensional:
- Valores nulos, registros duplicados
- Fechas fuera del periodo definido (2023-01 a 2025-12)
- Valores fuera de rangos razonables (ej. morosidad negativa o > 100%)
- Nombres de bancos inconsistentes entre fuentes (normalizar antes de cargar)
- Violaciones de la regla de unicidad (sección 5)

## 10. Checklist antes de escribir código de extracción masiva

- [ ] Cuenta AWS con usuario IAM (no root)
- [ ] Bucket S3 con carpetas `raw/bcrp/`, `raw/sbs/`
- [ ] Instancia RDS PostgreSQL (free tier) — probar conexión antes de escribir el pipeline
- [ ] Repositorio Git con la estructura exigida por el curso (`data/`, `notebooks/`, `src/`, `tests/`, `README.md`, `requirements.txt`)
- [ ] Códigos de series BCRP confirmados (tipo de cambio ✅, tasa de referencia, inflación pendientes)
- [ ] Los 2 pendientes de la sección 4.3 cerrados (asimetría temporal, verificación visual de Consumo/Hipotecario)

## 11. Plan de trabajo

| Semana | Foco | Entregable |
|---|---|---|
| 1 | Diseño y setup (✅ completado) | Este documento + auditoría estructural + decisiones de alcance |
| 2 | Extracción (E) | Cerrar pendientes de la sección 4.3, scripts reutilizables de BCRP y SBS → S3 raw, 36 meses completos |
| 3 | Transformación + calidad + carga | Star schema poblado en RDS, validaciones de la sección 9 aplicadas |
| 4 | Feature engineering + modelos ML | Features con rezago, split temporal, Regresión Lineal + Regresión Logística, métricas |
| 5 | Cierre del parcial | Informe según estructura del curso, consultas SQL de negocio (nivel 1 y 2), sustentación |

## 12. Riesgos y decisiones abiertas

- Confirmar con el profesor el alcance exacto exigible en el parcial (de las 29 secciones del informe final, cuáles aplican ya).
- SBS no tiene API — el scraping puede romperse si cambian el formato; guardar el Excel crudo en S3 con fecha de extracción.
- El dataset ML es pequeño (~180 filas) — gestionar expectativas de desempeño desde el diseño.
- Para el final grupal: alinear con el equipo qué fases asume cada integrante, para evitar que el trabajo ya avanzado genere desbalance de aporte.
