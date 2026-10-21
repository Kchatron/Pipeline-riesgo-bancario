# Etapa B: DAG manual histórico

`dags/historico_mensual.py` declara seis tareas lineales:

```text
ingest → verify_ingest → rebuild → verify_rebuild → publish → verify_version
```

Usa el SDK público `airflow.sdk` de Airflow 3.3.2. `HistoricoOperator`, en
`src/orquestacion/operador_historico.py`, crea un `AdaptadorCLI` por ejecución y
conecta `on_kill()` con `cancelar()`. No inicia procesos al importar el DAG.
Referencias oficiales: [operadores personalizados](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/custom-operator.html)
y [pruebas de DAG y operadores](https://airflow.apache.org/docs/apache-airflow/3.3.2/best-practices.html#testing-a-dag).

## Parámetros y ejecución

- `schedule=None`, `catchup=False`, `max_active_runs=1`, `max_active_tasks=1`.
- Cada tarea declara explícitamente `retries=0` y mantiene `all_success`.
- `periodo` es obligatorio, sin valor predeterminado: `YYYY-MM` entre
  `2023-01` y `2025-12`. Un `Param` de JSON Schema valida al activar el DAG y
  el operador vuelve a comprobarlo antes de invocar el adaptador. Véase
  [Params con schedule=None](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/params.html).
- Rutas del contenedor vigente: `/store/historico` y `/sources/sbs`.
  No se aceptan rutas desde la configuración de un DagRun; quedan en el operador.
- Cada DagRun ingiere un mes desde `initial`, que debe estar inicializado y
  verificado por separado. No inicializa infraestructura ni repara datos.

Relanzar todo el DAG para el mismo mes crea nuevos candidatos; no equivale a
repetir `publish` del mismo candidato. La publicación idempotente y el rechazo
de base obsoleta siguen perteneciendo a la CLI. `max_active_runs=1` serializa
DagRuns de este DAG; no sustituye el bloqueo ni la comprobación de base frente
a otros escritores CLI. No se añade `--force`, resolución de LATEST, reintentos
HTTP ni recuperación automática. Las alertas externas siguen aplazadas a v1.1.

## XCom e integridad entre tareas

Cada `execute()` devuelve exclusivamente una de estas estructuras, que Airflow
guarda como un solo `return_value` (`multiple_outputs=False`):

```json
{"candidate_id": "ingest-UUID_o_rebuild-UUID", "manifest_sha256": "64_hex"}
```

```json
{"version_id": "v-HASH", "manifest_sha256": "64_hex"}
```

No se transfieren rutas, CSV, logs ni manifiestos completos. Se recupera la
identidad de la tarea anterior del mismo DagRun, sin incluir ejecuciones previas.
El adaptador valida el [contrato JSON](cli_historico_json.md) de cada operación.
El operador comprueba además:

- `verify` devuelve el mismo identificador y hash recibidos.
- `rebuild` registra como origen el candidato ingerido y su hash verificado.
- El ID de `publish` corresponde a `v-` más el hash del manifiesto de rebuild.
  El hash que devuelve publish es el del manifiesto del paquete: no son el
  mismo documento ni se comparan sus hashes como si lo fueran.
- `verify_version` recibe exactamente el `version_id` y hash devueltos por
  `publish`. No consulta ni vuelve a resolver LATEST.

Los errores del adaptador se propagan sin cambiar sus códigos ni evidencia.
Un código 4 o una cancelación durante publicación pueden indicar un LATEST
modificado o incierto: requieren inspección y recuperación manual. No se emite
una identidad XCom de éxito. El control de cancelación también cubre `on_kill`
antes del arranque y durante la creación del adaptador.

## Pruebas aisladas con Airflow 3.3.2

`tests_airflow/test_historico_mensual.py` usa `unittest` de la biblioteca estándar.
No instala Airflow en el venv del pipeline. La importación mediante `DagBag`,
la serialización y las pruebas de operadores usan el Airflow real de la imagen
aprobada; `Popen` y las señales al pipeline se simulan. No se crea un DagRun ni
se necesita una base de metadatos para estas pruebas.

Desde la raíz del repositorio, copiar únicamente el código que se va a probar:

```sh
DAG_REVIEW=$(mktemp -d)
mkdir -p "$DAG_REVIEW/src/orquestacion" "$DAG_REVIEW/dags" "$DAG_REVIEW/tests_airflow"
cp src/__init__.py "$DAG_REVIEW/src/"
cp src/orquestacion/__init__.py src/orquestacion/adaptador_cli.py \
  src/orquestacion/operador_historico.py "$DAG_REVIEW/src/orquestacion/"
cp dags/historico_mensual.py "$DAG_REVIEW/dags/"
cp tests_airflow/test_historico_mensual.py "$DAG_REVIEW/tests_airflow/"
# Proporcionar el ID completo de la imagen local aprobada con Airflow 3.3.2.
AIRFLOW_DAG_TEST_IMAGE=sha256:REEMPLAZAR_POR_64_HEXADECIMALES
docker run --network none --read-only --tmpfs /tmp:rw,size=512m \
  --user "$(id -u):0" --env AIRFLOW_HOME=/tmp/airflow \
  --env AIRFLOW__CORE__LOAD_EXAMPLES=false \
  --env AIRFLOW__CORE__DAGS_FOLDER=/review/dags \
  --env PYTHONDONTWRITEBYTECODE=1 --env PYTHONPATH=/review --workdir /review \
  --mount "type=bind,src=$DAG_REVIEW,dst=/review,readonly" \
  --entrypoint python "$AIRFLOW_DAG_TEST_IMAGE" \
  -m unittest discover -s tests_airflow -v
```

El montaje contiene solo código, sin fuentes, almacenes ni secretos. Los avisos
`execute cannot be called outside of the Task Runner` corresponden a llamadas
unitarias directas; no se ocultan ni equivalen a ejecutar tareas en el scheduler.
Los resultados de prueba no demuestran todavía la cancelación real de un DagRun
ni persistencia real de XCom: el contexto y las respuestas CLI son simulados.

Validación del host, sin dependencias Airflow:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m pytest -p no:cacheprovider tests/
.venv/bin/python -B -m ruff check --no-cache src tests dags tests_airflow docker/airflow
git diff --check
```

## Integración pendiente

No se modifica Compose, Dockerfile, `.dockerignore`, el workflow ni los servicios.
La imagen actual aún no incorpora este DAG ni el adaptador. El montaje `/review`
es exclusivo de las pruebas, no una sustitución del código de servicios activos.
Antes de activar el DAG habrá que incorporar código/DAG en una imagen nueva,
validar su golden master y cancelación real, y seleccionarla explícitamente.
El CI vigente ejecuta `pytest tests/` y Ruff en `src/tests`; no ejecuta
`tests_airflow` ni valida Compose. No se atribuyen estas pruebas locales al CI.
