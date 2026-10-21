# Imagen del pipeline y Airflow: Etapa 2

No inicia servicios Airflow. No contiene Compose, DAG ni adaptador. Las pruebas
son sin red y sin Excel originales. La validación numérica reconstruye desde
las cinco referencias aprobadas; las ingestas de prueba usan fixtures simulados.
No hay entrenamiento.

## Identidades y entornos

- Base oficial: `apache/airflow:3.3.2-python3.14`, Linux **amd64**.
- Digest del manifiesto de imagen Linux amd64 (no del índice multiplataforma):
  `sha256:361bf4e851dffa0bbf7dc5dbe6a33da8718a4cf987326a8380ba435106dbb922`.
- Python observado: **3.14.7**; host validado: 3.14.8. No se presupone igualdad
  numérica entre parches: se exige de nuevo el golden master dentro de la imagen.
- Código de partida: merge `cfdc8a9` de la CLI JSON `7b9e369`. Esta etapa no
  cambia `src/`, pruebas previas, metodología ni referencias.
- El `source_commit` de `config/referencias_historico.json` identifica las
  referencias, **no** el código ejecutado. Los manifiestos existentes conservan
  `executed_code_commit: null`. El informe de validación registra por separado
  hashes de los archivos incorporados; se debe conservar también el ID de imagen.

Airflow conserva su Python y sus paquetes en `/home/airflow/.local`. Se reproducen
descargando el digest oficial: no se reinstalan ni se resuelven con el lock del
pipeline. `airflow-base.freeze.txt` registra los 437 paquetes de esa imagen,
incluido pip 26.2.1; es un inventario, no un reemplazo del digest ni un lock para
reinstalar Airflow sobre otro sistema. No se añaden providers.

`/opt/pipeline-venv` se crea durante el build, sin `--system-site-packages`.
`pipeline.lock` fija las 31 dependencias directas/transitivas de `requirements.txt`
y Ruff 0.17.1 (el mismo del CI). Las versiones proceden del venv validado; no se
actualizan al construir. Incluye pytest para ejecutar las comprobaciones en el
entorno real. pip, el bootstrap del venv, procede del Python del digest base.
No se instala Airflow dentro de este venv. `requirements.txt` permanece intacto.

La instalación usa `--require-hashes --only-binary=:all:`. Los hashes autorizan
los wheels publicados de cada versión, para distintas plataformas; pip selecciona
el compatible con el digest amd64. No se permiten sdists ni compilaciones locales.
El lock es completo para Linux/CPython 3.14; no se promete soporte en otro sistema.
`generate_pipeline_lock.py` documenta su generación desde el venv del host y los
hashes oficiales de PyPI. No se ejecuta durante el build y no instala paquetes.
Antes de leer requisitos,
consultar PyPI o escribir, exige Linux, CPython 3.14 y arquitectura amd64/x86_64;
en otro entorno termina con un mensaje claro y código distinto de cero:

```sh
.venv/bin/python -B docker/airflow/generate_pipeline_lock.py
```

Regenerarlo es una decisión explícita: depende del entorno instalado y PyPI puede
añadir wheels. Para reproducir esta instalación se usa el lock revisado, sin
regenerarlo. No incluye paquetes personales del host, por ejemplo python-docx.

## Construcción

Desde la raíz del repositorio:

```sh
docker pull apache/airflow:3.3.2-python3.14@sha256:361bf4e851dffa0bbf7dc5dbe6a33da8718a4cf987326a8380ba435106dbb922
docker image inspect apache/airflow:3.3.2-python3.14@sha256:361bf4e851dffa0bbf7dc5dbe6a33da8718a4cf987326a8380ba435106dbb922 --format '{{.Os}}/{{.Architecture}} {{json .RepoDigests}}'
docker build --platform linux/amd64 -f docker/airflow/Dockerfile -t historico-airflow:stage2 .
docker image inspect historico-airflow:stage2 --format '{{.Id}} {{.Size}}'
```

La inspección local se hace después del pull y sobre la misma referencia fijada
que `FROM`; debe mostrar `linux/amd64` y el RepoDigest indicado. No inspecciona
solamente el tag mutable ni pretende verificar otra vez el registro remoto.

Se usa el builder disponible; no requiere instalar Buildx. `.dockerignore` es una
lista cerrada: entran código, ancla de configuración, cinco CSV, requirements,
configuración Ruff, pruebas seleccionadas y archivos necesarios del build.
Quedan fuera `.git`, `.venv`, credenciales, documentos personales, Excel, otros
datos y almacenes. Las exclusiones de cada subdirectorio se repiten después de
permitir su padre para evitar que el builder clásico reincorpore su contenido;
`validate.py` exige inventarios exactos de datos y pruebas durante el build.
Las dos suites de ingesta se incorporan completas, pero al ejecutar se excluyen
exactamente dos casos que necesitan Excel originales (véase el desglose siguiente).
El directorio `data/raw/SBS` se crea vacío durante el build únicamente para que
los fixtures puedan enumerar los Excel presentes; no contiene archivos ni sustituye
a los fixtures de extracción. Las pruebas con Excel reales siguen fallando por
archivo ausente si se intentan ejecutar sin sus datos, no se saltan silenciosamente.

Código, configuración, pruebas y referencias quedan como archivos 0444 y
directorios 0555 propiedad de root. El venv tampoco es escribible por el usuario
de ejecución. No montar otra copia encima de `/opt/project` o del venv: el código
que pasó el golden master debe ser el ejecutado después. Esto protege frente al
UID de ejecución, no frente a root o al administrador Docker.

Durante el build se ejecutan `pip check` en ambos entornos y `validate.py` con
el Python del pipeline como usuario `airflow` (50000:0). Se conservan las evidencias
de ese golden master en `/tmp/build-golden-store-validation` de la imagen. No se
promete identidad byte a byte de la imagen, timestamps o identificadores UUID.
La exigencia es igualdad lógica y numérica exacta de los datos mediante lectura
float64 `round_trip`, sin tolerancias adicionales.

## Cobertura de ingesta sin Excel privados

La colección original del host contiene 300 casos. La selección inicial de la
imagen ejecutaba 198 y excluía estos 102 casos completos:

| Módulo antes excluido | Host | Simulados, ahora ejecutables en imagen | Con Excel originales, solo host/CI |
|---|---:|---:|---:|
| `tests/test_historico_ingest.py` | 83 | 82 | 1 |
| `tests/test_historico_ingest_rebuild.py` | 19 | 18 | 1 |
| Total | 102 | 100 | 2 |

Los conteos proceden de `pytest --collect-only`, no del total por diferencia.
Los 82 casos de ingesta usan archivos temporales controlados y funciones SBS
simuladas; BCRP usa el extractor real con `urlopen` simulado. Cubren meses,
esquemas, claves, límites decimales, aislamiento, informes, fallos y el orden
de columnas de BCRP. Los otros 18 cubren ingesta→reconstrucción/verificación,
entradas efectivamente utilizadas, golden master, P75, alteraciones y evidencia.
La variante `[controlados]` del flujo CLI está incluida.

Se excluyen únicamente los dos node ID explícitos del comando de validación
siguiente. La variante `[reales]` y `test_integracion_excel_sbs_reales` necesitan tres Excel del
host; aun en esos casos la API BCRP está simulada. El CI actual ejecuta
`python -m pytest tests/` con los archivos versionados y mantiene esos dos casos.
No se modifica el workflow. La imagen ajustada recoge 300 casos y ejecuta
**298**, con **2 deseleccionados** y sin red (`--network none`). No se usa un
filtro genérico por la palabra `reales`: eso excluiría también concurrencia.

Las guardas del generador se prueban aparte, sin generar otro lock ni consultar
PyPI; este archivo de pruebas de la herramienta no entra en la imagen:

```sh
.venv/bin/python -B -m pytest -p no:cacheprovider docker/airflow/test_generate_pipeline_lock.py
```

## Validación sin root sobre ext4

Crear **otro directorio nuevo** sobre disco local persistente, fuera del repositorio
y de `/tmp`. No reutilizar los almacenes de fuentes reales anteriores. Ejemplo
(ajustar únicamente el directorio padre al disco ext4):

```sh
PIPELINE_EVIDENCE=$(mktemp -d "$PWD/../airflow-imagen-stage2-XXXXXXXX")
PIPELINE_UID=$(id -u)
findmnt -T "$PIPELINE_EVIDENCE"
df -h "$PIPELINE_EVIDENCE" /var/lib/docker
docker system df
```

El UID debe ser no root. El directorio pertenece a ese UID. El montaje completo
`/validation` contiene almacenes, temporales y evidencia en el mismo ext4.
No se usa el venv del host. Las pruebas siguientes tienen la red desactivada;
no se instala nada al arrancar. No llevan `--rm`: los contenedores y la evidencia
se conservan para inspección. Usar nombres nuevos al repetir la validación.

Comprobar ambos entornos sin instalar y registrar versiones:

```sh
docker run --name historico-stage2-checks --network none --user "$PIPELINE_UID:0" \
  --entrypoint bash historico-airflow:stage2 -c \
  'python --version && python -m pip check && /opt/pipeline-python -m pip check && /opt/pipeline-python -m pip freeze --all'
```

Comprobar herencia desde un padre con umask 0002, UID/GID, aislamiento, permisos,
hashes, CLI `init → verify → rebuild(initial) → verify`, 340 × 23 y diez P75:

```sh
docker run --name historico-stage2-golden --network none --read-only \
  --user "$PIPELINE_UID:0" --tmpfs /tmp \
  --mount "type=bind,src=$PIPELINE_EVIDENCE,dst=/validation" \
  --entrypoint bash historico-airflow:stage2 -c \
  'umask 0002; exec /opt/pipeline-python docker/airflow/validate.py --store /validation/golden-store --require-ext4 --require-uid '"$PIPELINE_UID"
```

`validate.py` exige umask 0022 y registra comandos, stdout/stderr separados,
identidad del candidato, P75 observados/esperados, versiones y hashes del código
y referencias en `golden-store-validation/`. Se detiene ante fallos y conserva
evidencia. El verificador de producción y las pruebas independientes comprueban
la población enero 2023–diciembre 2024, 24 observaciones por cada uno de diez paneles.

Ejecutar las pruebas incorporadas con ese mismo Python/UID/almacenamiento,
excluyendo por node ID los dos casos que necesitan Excel originales:

```sh
docker run --name historico-stage2-tests --network none --read-only \
  --user "$PIPELINE_UID:0" --tmpfs /tmp \
  --mount "type=bind,src=$PIPELINE_EVIDENCE,dst=/validation" \
  --entrypoint /opt/pipeline-python historico-airflow:stage2 \
  -m pytest -p no:cacheprovider --basetemp /validation/pytest \
  --deselect tests/test_historico_ingest.py::test_integracion_excel_sbs_reales \
  --deselect 'tests/test_historico_ingest_rebuild.py::test_flujo_cli_ingest_rebuild_verify_usa_consolidados[reales]' tests/
```

Incluye reconstrucción, P75 observado, JSON/compatibilidad, publicación,
concurrencia con procesos reales, fallos de sincronización, códigos inciertos,
sellado pendiente tras rename y recuperación explícita con IPC; también rechazo
de contenidos/permisos alterados y ausencia de duplicados. No simula resistencia
real a cortes eléctricos. Fuera del contenedor se mantiene `pytest tests/`
completo con sus Excel existentes: no se sustituye por esta selección.

El lanzador `/opt/pipeline-python` aplica umask 0022 **solo a sus procesos** y
ejecuta `/opt/pipeline-venv/bin/python`. La finalización pendiente reconoce
estrictamente la raíz 0755; heredar umask 0002 al crear esa raíz puede producir
0775 y bloquear su recuperación. En la futura integración, el subproceso CLI
debe invocarse mediante este lanzador, sin cambiar la umask global de Airflow.
Ese adaptador y la cancelación de subprocesos siguen pendientes.

Guardar resultados y medir al terminar, sin limpieza:

```sh
docker logs historico-stage2-golden
docker logs historico-stage2-tests
docker image inspect historico-airflow:stage2 --format '{{.Id}} {{.Size}}'
docker system df
df -h "$PIPELINE_EVIDENCE" /var/lib/docker
du -sh "$PIPELINE_EVIDENCE"
```

No hay CI remoto nuevo en esta etapa. Las alertas externas siguen aplazadas a
v1.1; Compose, DAG, servicios y entrenamiento están fuera de este cambio.

## Validación inicial, anterior a los ajustes de revisión (20 de octubre de 2026)

Los resultados e ID siguientes pertenecen a la imagen inicial de nueve módulos.
La ampliación a once módulos requiere reconstruir y validar otra imagen; no se
atribuyen los 198 resultados anteriores a la imagen ajustada.

- Imagen local `historico-airflow:stage2`, ID
  `sha256:57913fb493fad8488c8e1d06af9b84076062b71f15f4d1c513cca8f364a1866c`.
- Tamaño informado por Docker: 3.899.189.126 bytes (3,90 GB; 3,63 GiB).
- `pip check` correcto en ambos entornos, durante build y como UID 1000:GID 0.
- Golden master del build correcto como UID 50000:GID 0.
- Golden master final sobre ext4 correcto como UID 1000:GID 0, raíz del contenedor
  de solo lectura y red desactivada: candidato
  `rebuild-4296b58bc7074380875a7fcbaa8d21be`, 340 × 23, diez P75 exactos.
- Inventario final comprobado: 34 archivos bajo src/config/data/tests/docker,
  incluidos exactamente cinco CSV y nueve módulos de prueba; coinciden con el
  host. Lock y lanzador copiados de la imagen también coinciden con sus originales.
- 198 pruebas del contenedor pasan en 182,84 s, incluidas la recuperación del
  sellado pendiente y la terminación de proceso con IPC. 300 pruebas del host
  pasan en 203,07 s. Ruff y `git diff --check` correctos.
- Un padre con umask 0002 conserva 0002 después de ejecutar el lanzador; su
  proceso Python recibe 0022. No se modifica la umask global de Airflow.
- Hashes de las cinco referencias y de todos los archivos previos intactos.

Mediciones del host, sin limpieza (incluyen evidencia e intentos anteriores):

| Recurso | Antes | Después |
|---|---:|---:|
| Libre en ext4 `/dev/nvme0n1p5` (`df -h`, redondeado) | 45 GiB | 40 GiB |
| Imágenes (`docker system df`) | 13 / 4,142 GB | 17 / 8,09 GB |
| Contenedores / activos | 6 / 3 | 15 / 3 |
| Volúmenes / activos / tamaño | 3 / 2 / 161,5 MB | Sin cambios |
| Build cache informada | 0 B | 0 B |

La evidencia externa ocupa 624 MiB, incluidos los archivos de las pruebas; los
tamaños Docker comparten capas y no equivalen a sumar tamaños virtuales de las
imágenes. No se detuvieron ni eliminaron recursos preexistentes.

El primer comando con `--progress plain` fue rechazado porque no hay Buildx;
se usó `docker build` clásico. Una primera regla de exclusión reincorporó los
Excel al permitir su directorio padre. Se detectó inspeccionando la imagen, se
detuvo únicamente ese contenedor de pruebas y se conservó su evidencia. El
contexto final corregido bajó de 5,672 MB a 555 kB y su inventario exacto se
comprueba antes de inicializar. Esa ejecución inicial no se cuenta como resultado
final. No se modificaron los Excel originales ni se hicieron consultas BCRP.

Los logs finales son los de `historico-stage2-*-final`. En el directorio externo
de evidencia quedan `container-commands.json` (comandos reales, usuario, montajes,
ID de imagen y salidas), `build-filtered.log`, mediciones, logs por contenedor y
`golden-store-final-validation/result.json`, además de las salidas JSON de cada
comando CLI y la auditoría P75 completa. Los directorios y contenedores anteriores
se conservan. No se midió ROC-AUC ni se ejecutó entrenamiento.

## Resultado de los ajustes de revisión

- Imagen validada: `historico-airflow:stage2-review`, ID
  `sha256:4f0f4342e2633f7da00a4a8b5d89a547b3884b2774dd5a3e1ef745d57d6bcc6d`.
  Se conserva el tag y la imagen inicial `historico-airflow:stage2`.
- Guardas del generador: 9 pruebas pasan (0,09 s), sin red ni escritura del lock.
  El entorno real Linux / Python 3.14.8 / x86_64 también pasa la guarda.
- Host: 300 pruebas pasan (201,70 s). Contenedor: 298 pasan y 2 deseleccionadas
  (250,76 s), incluidos los 100 casos simulados añadidos.
- Build y golden master no root sobre ext4 correctos: 340 × 23 y diez P75 exactos;
  candidato `rebuild-8c65462706e746e4938c80f534316428`. Inventario exacto de cinco CSV
  y once módulos de prueba, con código idéntico al host y sin Excel originales.
- Ruff y `git diff --check` correctos. No se modifican `src/`, pruebas anteriores,
  referencias, `requirements.txt` ni `pipeline.lock`.

La inspección local por la referencia fijada devuelve `linux/amd64` y su
RepoDigest. El índice del tag consultado enumera ese digest como
`application/vnd.oci.image.manifest.v1+json`, plataforma Linux amd64, no como
índice. En esta revisión `docker manifest inspect` por digest, con y sin
`--verbose`, falló con `manifest verification failed for digest ...`. Se conserva
esa limitación: no se afirma una nueva verificación criptográfica remota. No se
cambió el digest ni `FROM`; el comando documentado inspecciona la imagen local
después del pull.

La revisión final del generador rechaza implementaciones distintas de CPython.
La batería de la herramienta contiene ahora 10 casos, incluido PyPy simulado
con Linux, versión 3.14 y x86_64 válidos; comprueba el rechazo antes de leer
requisitos, acceder a PyPI o escribir. Los 9 casos citados arriba pertenecen a
la ejecución anterior. No se regeneró el lock ni se cambió la imagen validada.
