# Validación de Airflow: CI y evidencia operativa

## Cobertura del workflow

`.github/workflows/ci.yml` define dos jobs independientes, sin `needs`, en
`push` y `pull_request`:

| Job | Comprobaciones |
|---|---|
| `lint-and-test` | Python 3.14, `requirements.txt`, Ruff 0.17.1 sobre `src/`, `tests/`, `dags/`, `tests_airflow/` y `docker/airflow/`; `pytest tests/` del pipeline. |
| `airflow-image-and-tests` | Build Linux amd64 sin caché de instrucciones y ejecución de las 16 pruebas `tests_airflow` dentro de la imagen obtenida. |

GitHub proporciona una [VM nueva por job](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
El segundo job no depende del venv ni de las instalaciones Docker del operador.
Construye el [Dockerfile](../docker/airflow/Dockerfile) existente: base Airflow
3.3.2 / CPython 3.14 fijada por digest y pipeline en `/opt/pipeline-venv`, con
`pipeline.lock` y hashes. No añade Airflow a `requirements.txt`.

El Dockerfile ejecuta `pip check` en ambos entornos, el golden master exacto
(340 × 23, diez P75 de 24 observaciones por panel) y las 16 pruebas SDK. El job
repite esas mismas 16 pruebas en un contenedor nuevo con el Python de Airflow;
no son 32 casos distintos. Usa el ID registrado mediante `--iidfile`, UID del
runner/GID 0, raíz de solo lectura, sin red ni montajes del host y `/tmp` en
tmpfs de hasta 512 MiB. Ese contenedor tiene límites de 1 GiB de RAM y 2 CPU.

Las pruebas SDK comprueban importación/serialización del DAG, dependencias,
parámetros, identidades XCom, errores y conexión `on_kill`, con CLI y contexto
simulados. **CI no levanta Compose/PostgreSQL, no crea DagRuns reales, no usa
Excel privados ni consulta BCRP.** El build solo usa los cinco CSV aprobados;
`.dockerignore` excluye secretos, documentos personales y almacenes externos.

## Tiempo, recursos y límites

El job tiene un límite de 30 minutos. Registra tiempo de build, tamaño lógico
de la imagen y RAM/disco antes y después, incluso si falla. Esas instantáneas
no miden el pico de RAM del builder. Los límites de 1 GiB/2 CPU corresponden
al contenedor SDK, no al build.

Un runner limpio necesita red para descargar la base por digest y los wheels
fijados. `--no-cache` obliga a repetir instalaciones y validaciones; no descarga
de nuevo una base ya presente. La imagen operativa validada ocupa unos 3,9 GB
lógicos; capas compartidas, temporales de pip y caché de construcción requieren
espacio adicional. No se suman tamaños lógicos como si fueran uso físico.
Los recursos de los runners dependen del tipo de repositorio; las mediciones
del host no garantizan tiempos ni disponibilidad idénticos en GitHub.

### Comprobación local de este cambio

Build `--no-cache --platform linux/amd64` sobre ext4, con la base ya disponible
y el builder clásico de Docker 29.9.1. Se descargaron de nuevo los wheels; no
se instaló Buildx ni se reiniciaron servicios. Resultado:

| Medida | Resultado |
|---|---|
| Build completo | 180,4 s; ambos `pip check`, golden master y 16 pruebas SDK correctos. |
| Imagen nueva | `sha256:0beacbcc1316bb7f356cb52562559e917f2d514440ffa8ec1a8b0d131c2a8108`; 3.894.822.902 bytes lógicos. |
| Contenedor SDK separado | 16/16; 8,0 s con arranque incluido, código 0 y sin OOM, bajo los límites del job. |
| Disco libre del filesystem | 36,67 → 36,03 GiB; variación neta aproximada de 664 MiB, con capas compartidas. |
| RAM disponible del host | 4,4 → 5,2 GiB sobre 15 GiB; swap usada 2,5 → 3,4 GiB. Son instantáneas, con otros servicios activos. |
| Pipeline en el host | `pytest tests/`: 369/369 en 187,12 s. |

Las mediciones y logs quedan fuera del repositorio, en el directorio nuevo
`airflow-ci-validation-opsjqfvu/`. Se conservaron los 23 contenedores operativos
(identidad, imagen, inicio y reinicios) y los hashes/permisos de los 2.399
artefactos del snapshot de almacenes. La imagen y el contenedor de prueba
terminado se conservan; no se ejecutó limpieza.

Los 30 minutos son un presupuesto inicial para CI, no una garantía derivada de
este build local. Falta medir un runner remoto sin la base disponible, su pico
de RAM y su consumo máximo de disco durante la construcción.

La verificación remota de esta rama queda pendiente hasta ejecutar Actions.
Se conserva la [limitación histórica de inspección remota del digest](../docker/airflow/README.md#resultado-de-los-ajustes-de-revisión);
no se cambia `FROM` ni se presenta una inspección local como nueva verificación
criptográfica del registro.

## Evidencias manuales locales

Pruebas del 21 de octubre de 2026, con Airflow 3.3.2, Linux/ext4 y la imagen
`sha256:7f1d4f03341eaa3f5cf5ecb96e1ec326870380cdd2811bdb934aaebc78d9aa8f`.
Los identificadores permiten localizar la evidencia externa; no se incorporan
contraseñas, almacenes, fixtures operativos ni logs completos al repositorio.

| Prueba | Resultado observado |
|---|---|
| Real `2024-06`, `manual__real_2024-06_20261021T184830Z_fbde38ab` | Excel SBS y API BCRP reales; seis tareas exitosas, un intento por tarea; 81 comparaciones sin diferencias; versión verificada, 340 × 23 y diez P75 exactos. |
| Repetición, `manual__repeat_2024-06_20261021T185720Z_058d3024` | Nuevos candidatos y versión, datos equivalentes; los 174 archivos de la versión anterior, sus permisos y la evidencia previa quedaron intactos. |
| Excel ausente, `manual__missing_sbs_2024-06_20261021T190818Z_e68d8461` | Copia SBS sin `B-2362-2024-06.xls`; `ingest` falló con código CLI 1, cinco tareas `upstream_failed` con cero intentos; `LATEST` idéntico en bytes/hash. Sin consulta BCRP. |
| Cancelación IPC, `manual__cancel_ipc_20261021T195120Z_958e2df1` | Tarea real y subproceso/descendiente de prueba; API Airflow → `on_kill` → `cancelar`; grupo terminado, hijo esperado, PIDs desaparecidos y sin escrituras posteriores ni XCom de éxito. |
| Antes de `LATEST`, `manual__publish_cancel_before_20261021T201025Z_c89e3e7a` | Publicador real desde referencias; `LATEST` conservó bytes/hash y resolvió la base. Nueva versión completa huérfana verificada; recuperación CLI explícita publicó esa versión sin duplicarla. |
| Después de `LATEST`, `manual__publish_cancel_after_20261021T201107Z_a9674cd8` | El reemplazo nativo ya había ocurrido: Airflow terminó `failed`, pero `resolve` devolvió la versión nueva. Ambas versiones verificaron; recuperación explícita devolvió `already_current=true`, sin duplicados. |

Los casos IPC emplearon montajes y almacenes exclusivos de prueba, conservando
los archivos de la imagen. Cada cancelación tuvo un intento, sin XCom de éxito
ni retry. La preparación de los dos casos de publicación tuvo un DagRun fallido
por permisos del launcher y una colisión de socket del controlador sin nuevo
trigger; se conservaron y corrigieron solo los fixtures externos. Esos fallos
de preparación no se cuentan como cancelaciones validadas.

Las dos ingestas reales partieron de `initial`: repetir el DAG no demuestra un
replay acumulativo de 36 meses ni equivale a repetir `publish` del mismo candidato.
Las versiones nuevas pueden tener identificadores distintos con datos iguales.

## Visibilidad, durabilidad y pendientes

`os.replace` cambia atómicamente la identidad visible de `LATEST`. Cancelar
después de ese syscall puede dejar visible la versión nueva antes del `fsync`
del directorio. **Visibilidad no equivale a durabilidad confirmada.** En las
pruebas el publicador terminó por SIGKILL (`-9`), sin respuesta CLI de éxito;
no se dedujo un código 4 que el proceso no alcanzó a devolver. No fueron
experimentos de pérdida eléctrica.

Se preservaron primero bytes, hashes, inventarios, constancias y logs; después
se ejecutaron `resolve`, `verify --version` y, por separado, la recuperación
manual `publish --resume-version`. No se usó un retry del DAG para recuperarse
ni se borraron versiones o temporales.

Queda pendiente la **concurrencia operativa** de Airflow con un escritor CLI
externo y un lector que mantiene la versión resuelta: base obsoleta, exclusión
mutua y lectura consistente bajo esa carrera real. Existen pruebas automáticas
con procesos CLI, pero `max_active_runs=1` solo serializa DagRuns del mismo DAG;
no sustituye `flock` ni la comprobación de base frente a otros escritores.
También quedan fuera de estas pruebas SIGKILL del propio TaskRunner, procesos
que abandonen su grupo y un replay acumulativo completo de los 36 meses.
