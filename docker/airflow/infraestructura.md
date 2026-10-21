# Etapa A: infraestructura local e inicialización

Esta etapa utiliza la imagen aprobada; no construye otra, no añade DAG ni
adaptador, no consulta BCRP y no ejecuta reconstrucción, publicación o entrenamiento.
El [Compose oficial de Airflow 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/docker-compose.yaml)
se adapta a LocalExecutor, con migración explícita que propaga errores y sin los
servicios Celery/Redis/worker/triggerer ni el `chown -R` del ejemplo oficial.

## Imágenes

- Pipeline/Airflow: ID local completo e inmutable proporcionado mediante
  `--image-id`, de una imagen previamente construida y validada.
  Airflow 3.3.2 y CPython 3.14.7; el venv aislado sigue en `/opt/pipeline-venv`.
- PostgreSQL: `postgres:16.15-bookworm`, digest del índice oficial
  `sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4`.
  Compose fija `linux/amd64`; el descriptor de esa plataforma en el índice es
  `sha256:ec9f656f17c8b4dfebf49e58cf5c46d493d4846d4ea6d8b85d0637f68ee33bfc`.
  Se comprobó `postgres --version`: 16.15, Debian 16.15-1.pgdg12+2.

`pull_policy: never` impide sustituir imágenes al arrancar. Para otra máquina,
hay que disponer de esa imagen de pipeline validada y descargar PostgreSQL por
la referencia completa fijada:

```sh
docker pull postgres:16.15-bookworm@sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4
```

No se usa el Dockerfile durante esta etapa: contiene una validación de
reconstrucción durante build que está fuera de esta autorización. Si falta la
imagen aprobada, se debe preparar y validar en una etapa autorizada para ello.

## Servicios, montajes y permisos

| Servicio | Función | Límite RAM |
|---|---|---:|
| `postgres` | Metadatos, volumen propio, sin puertos publicados | 512 MiB |
| `airflow-init` | `airflow db migrate`, termina con su código real | 768 MiB |
| `api-server` | UI, autenticación y Execution API, un worker | 1024 MiB |
| `scheduler` | LocalExecutor, `parallelism=1`, hostname interno `scheduler` | 1536 MiB |
| `dag-processor` | Un proceso de parsing; sin DAG en esta etapa | 768 MiB |
| `data-init` | CLI `init` y `verify initial`, manual y sin red | 768 MiB |

Los cuatro servicios persistentes suman límites de 3840 MiB. Son límites,
no mediciones ni reservas de RAM. La migración y data-init son puntuales.
PostgreSQL usa el usuario y entrypoint oficiales para su volumen nuevo; los
servicios Airflow y data-init usan el UID del host y GID 0.

- `postgres` debe estar saludable antes de migrar; los componentes Airflow
  esperan una migración exitosa. El scheduler también espera la API saludable.
- La API se publica únicamente como `127.0.0.1:8080` por defecto. PostgreSQL solo
  es accesible en la red privada de este proyecto Compose, sin colisión con
  otras bases que usen el puerto 5432 del host.
- `/opt/project` y `/opt/pipeline-venv` proceden de la imagen aprobada. El código
  del pipeline no es escribible por el UID de ejecución. No se montan encima
  copias del host ni el `.venv` del host.
- `AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags`, carpeta vacía ya presente en
  la imagen oficial. No se monta una carpeta DAG del host ni se crea un DAG.
  En la etapa posterior, el DAG se incorporará a esa ruta en la imagen y se
  fijarán sus permisos de solo lectura, validando la nueva imagen antes de usarla.
- La raíz nueva de datos se monta una sola vez en `/store`, únicamente en
  scheduler y data-init, con escritura. La CLI usa **`/store/historico`**:
  `init` exige que su ruta no exista, mientras Docker exige una raíz de montaje
  existente. `work/`, futuras `versions/`, `.publish.lock` y `LATEST` estarán
  juntos bajo ese subdirectorio, en el mismo ext4. No se cambia la CLI.
- Solo el scheduler monta los Excel originales en `/sources/sbs`, con
  `read_only: true`; aquí no se procesan. API y DAG processor no montan ni
  fuentes SBS ni almacén histórico.
- Los logs externos se comparten en `/opt/airflow/logs`. API y scheduler
  utilizan el mismo secreto para el acceso a logs; el puerto interno 8793
  del scheduler no se publica al host. Su hostname estable es resoluble desde
  la API. El contenido de logs y autenticación persiste fuera del repositorio.
- No hay cambios recursivos de dueño/permisos sobre datos existentes.
  `prepare_local.py` asigna 0700/0600 solo a directorios/archivos nuevos.
  `/opt/pipeline-python` aplica umask 0022 solo a sus procesos; no se cambia
  globalmente la umask de Airflow.

## Preparación nueva y secretos

Desde la raíz del repositorio, elegir un padre externo existente en ext4. El
helper comprueba el filesystem, los cinco hashes, la imagen local, el directorio
SBS y la disponibilidad del puerto; registra RAM/disco antes de iniciar servicios.
No instala paquetes ni inicia contenedores y nunca reutiliza otro almacén:

```sh
.venv/bin/python -B docker/airflow/prepare_local.py \
  --parent /ruta/externa/en/ext4 --port 8080 \
  --image-id 'sha256:REEMPLAZAR_POR_64_CARACTERES_HEXADECIMALES'
```

El argumento `--image-id` es obligatorio. Utilizar el ID completo registrado al
validar la imagen, no una etiqueta ni un ID abreviado. El helper ejecuta
`docker image inspect` sobre ese ID y exige identidad exacta y plataforma
Linux amd64 antes de crear estado. Escribe ese mismo ID en `AIRFLOW_IMAGE` de
`local.env` y en `evidence/before.json`; no descarga ni reconstruye imágenes y
no tiene una etiqueta alternativa. Un build válido puede producir otro ID:
debe validarse primero y proporcionarse explícitamente. Esta inspección solo
comprueba existencia, identidad y plataforma; no sustituye las pruebas del
pipeline exigidas para aprobar la imagen.

Se puede pasar `--sbs-dir RUTA`; por defecto usa `data/raw/SBS` de este repositorio.
El JSON de salida contiene rutas y nombre de proyecto, nunca contraseñas.
Se genera un directorio `airflow-local-*` con:

| Ruta relativa | Contenido |
|---|---|
| `local.env` | Configuración: UID, puerto, imagen, rutas, proyecto único |
| `secrets/postgres.env` | Contraseña PostgreSQL aleatoria |
| `secrets/airflow.env` | Conexión SQL, Fernet, JWT y secreto de logs persistentes |
| `auth/passwords.json` | Contraseña aleatoria del usuario local `admin` |
| `logs/` | Logs compartidos |
| `data/` | Raíz vacía montada en `/store` |
| `evidence/` | Mediciones y verificaciones, fuera de Git |

Autenticación: SimpleAuthManager para desarrollo local, con
`simple_auth_manager_all_admins=false`. La contraseña se prepara antes del primer
inicio para que Airflow no la genere/imprima en sus logs. El estado de autenticación
se monta solo en la API; SimpleAuthManager requiere abrir ese archivo en modo
lectura/escritura. Ninguna contraseña está en Compose o en la plantilla
[local.env.example](local.env.example). No volcar `config` sin `--quiet`,
variables de entorno, contraseñas o tokens en informes ni archivos versionados.
El acceso al archivo local de contraseña queda reservado al operador del host.
Si se configura otro `AIRFLOW_ADMIN_USER`, su entrada de contraseña debe estar
preparada en `auth/passwords.json` antes del arranque para evitar la generación
automática de una contraseña en logs.

## Inicio e inicialización

Usar en todas las órdenes el `local.env` externo generado. Ejemplo en Bash/zsh:

```sh
AIRFLOW_LOCAL_ENV=/ruta/externa/airflow-local-XXXX/local.env
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml config --quiet
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml up -d --wait postgres
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml up -d airflow-init
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml wait airflow-init
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml up -d --wait --wait-timeout 240
```

El arranque normal no incluye `data-init`, que tiene `profiles: [data]` y no es
dependencia de otro servicio. `airflow-init` puede volver a ejecutar la migración
idempotente al resolver dependencias durante un `up`; no ignora errores.

Solo después, ejecutar manualmente el servicio de datos:

```sh
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml \
  --profile data run --no-deps data-init
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml \
  exec -T scheduler /opt/pipeline-python -m src.pipeline.historico verify \
  --store /store/historico --candidate initial --json
```

data-init emite dos documentos JSON sucesivos: uno por `init` y otro por `verify`.
Si `init` falla, no ejecuta verify. Repetir data-init sobre el almacén ya creado
produce el error de la CLI; no lo sobrescribe ni realiza reparación silenciosa.
Para conservar los contenedores puntuales como evidencia no se usa `--rm`.

La UI queda en `http://127.0.0.1:8080`; `AIRFLOW_PORT` permite elegir otro puerto.
No hay DAG ni DagRuns, por lo que la visualización real de logs de una tarea
desde la UI solo podrá probarse en la etapa del DAG. Aquí se comprueban el
montaje compartido, lectura desde la API y conexión al servidor interno de logs.

## Verificación, persistencia y límites de pruebas

```sh
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml ps --all
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml exec -T scheduler airflow db check
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml restart postgres api-server scheduler dag-processor
# Esperar de nuevo a los healthchecks antes de verificar initial y hacer login.
docker compose --env-file "$AIRFLOW_LOCAL_ENV" -f docker/airflow/compose.yml exec -T scheduler \
  /opt/pipeline-python -m src.pipeline.historico verify --store /store/historico --candidate initial --json
docker system df
free -h
df -h /var/lib/docker /ruta/externa/airflow-local-XXXX
```

La comprobación compara los hashes de initial, autenticación y secretos antes y
después del reinicio; comprueba revisión de migración persistente, logs y login,
UID/GID, montaje SBS solo lectura y ausencia de almacén en API/DAG processor.
No se elimina ninguna versión, volumen, contenedor preexistente o evidencia.
Nunca se usa `down -v` ni se reutilizan los almacenes históricos anteriores.

Pruebas autorizadas de esta etapa:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m pytest -p no:cacheprovider \
  tests/test_historico_init.py docker/airflow/test_generate_pipeline_lock.py \
  docker/airflow/test_prepare_local.py
.venv/bin/python -B -m ruff check --no-cache src tests docker/airflow
git diff --check
```

La suite completa `pytest tests/` se omite aquí porque ejecuta reconstrucción y
publicación en temporales, prohibidas en esta etapa. Los resultados anteriores
de 300/298 pruebas de la imagen no se presentan como una ejecución nueva.
No se añaden `tests_airflow`, adaptador, DAG, workflow ni dependencias.
