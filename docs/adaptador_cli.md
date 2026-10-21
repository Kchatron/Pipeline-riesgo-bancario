# Etapa B: adaptador de la CLI, sin DAG

`src/orquestacion/adaptador_cli.py` usa solo biblioteca estándar. No importa
Airflow, pandas ni los módulos de datos; importarlo no ejecuta procesos.
Una instancia representa una operación; el futuro operador creará una por tarea.

```python
from src.orquestacion.adaptador_cli import AdaptadorCLI

adaptador = AdaptadorCLI()
# Ejemplo de uso futuro dentro de la imagen, no una ejecución de esta etapa:
resultado = adaptador.ejecutar(
    "ingest", store="/store/historico", candidato="initial",
    mes="2024-06", sbs_dir="/sources/sbs",
)
identificador = resultado["candidate_id"]
```

Admite las operaciones del DAG acordado: `ingest`, `verify`, `rebuild`, `publish`.
`verify` acepta candidato o versión, mutuamente excluyentes; sin selector usa
`initial`. No incorpora inicialización de infraestructura/datos, `resolve`,
reanudación automática ni reintentos. La recuperación manual sigue en la CLI.

Invoca una lista de argumentos, sin shell, con directorio `/opt/project`:
`/opt/pipeline-python -m src.pipeline.historico OPERACION --store RUTA --json ...`.
El lanzador aprobado aplica umask 0022 solo al pipeline y usa su venv aislado.
No cambia la umask ni dependencias del proceso llamador.

## Contrato y errores

Se comprueba primero el código de salida. `ErrorCLI` conserva `returncode`,
`stdout` y `stderr`, incluidos códigos 1/2/3/4 y terminación por señal. El código
4 nunca se interpreta como publicación fallida con LATEST intacto. No hay
reintentos, correcciones, borrados ni interpretación de mensajes humanos.

Con código cero se exige un único objeto JSON UTF-8, sin claves duplicadas ni
constantes NaN/Infinity. Se valida el [contrato esquema 1](cli_historico_json.md):
operación, campos, estado, etapa, formatos de identificadores y hashes, ruta del
destino, mes y origen cuando corresponden. Una respuesta incompatible produce
`ErrorContratoCLI`, sin devolver éxito.

`verify` debe devolver exactamente el selector solicitado. `ingest` y `rebuild`
deben devolver el identificador nuevo y origen de su operación. En `publish`
se conserva la identidad devuelta por ese publicador y su `already_current`;
no se calcula la versión a partir del hash del manifiesto del paquete (son
hashes de documentos distintos). Nunca se consulta LATEST para sustituir IDs.
El adaptador no verifica por su cuenta el contenido de manifiestos: para eso
se mantienen las tareas `verify` del flujo acordado.

stdout queda reservado al resultado capturado y decodificado; no se imprime.
stderr se captura por separado y se reenvía al stderr del llamador al terminar,
incluida evidencia de errores. No hay streaming de progreso en esta primera
parte. Ante un error se conservan ambas salidas en `ErrorCLI`; bytes inválidos
de diagnóstico se representan con reemplazo para no ocultar el código de salida.
Un éxito exige UTF-8 estricto en stdout.

## Cancelación

Cada subproceso nace con `start_new_session=True`. `cancelar()` marca la operación
y envía SIGKILL al grupo propio. Se elige terminación forzada, sin periodo de
gracia ni temporizadores: el pipeline ya contempla interrupciones abruptas y
conserva evidencia; no se añade un protocolo de parada al código de datos.
El hilo que ejecuta `ejecutar()` espera al hijo mediante `communicate()` y
rechaza éxito aunque haya recibido código cero y JSON válido durante la carrera
con la cancelación. Cancelar antes del arranque impide crear el proceso.
El candado es reentrante para que un `on_kill` en el mismo hilo pueda marcar
cancelación durante `Popen` o la validación sin bloquearse. Si `Popen` aún no
había devuelto el proceso, se vuelve a comprobar la marca al obtenerlo: se mata
su grupo antes de esperarlo. También se comprueba la marca después de validar
el JSON, antes de devolver el resultado.
Si `communicate()` es interrumpido por una excepción, se mata el grupo, se espera
al hijo y se propaga la excepción original; un fallo de limpieza queda anotado.

En el futuro operador se conectará `on_kill` a `cancelar()`. El adaptador no
instala manejadores globales de señales. No puede manejar SIGKILL del propio
orquestador ni contener descendientes que deliberadamente abandonen el grupo.
No se afirma que el retorno de `cancelar()` equivalga a haber esperado al hijo:
esa espera corresponde a `ejecutar()`. Cancelar publicación puede dejar LATEST
cambiado o estado incierto: hay que inspeccionar evidencia y usar la recuperación
explícita existente, nunca relanzar automáticamente el DAG.

Las pruebas de esta parte simulan Popen, señales y cancelaciones; comprueban la
lista exacta del comando, el grupo destinatario, la espera y los contratos.
Las dos carreras reentrantes se inyectan en el mismo hilo dentro de `Popen` y
del validador. Cada caso corre en un worker separado, con IPC y un límite de
espera para detectar un bloqueo sin dejar colgado pytest; no se usan pausas
para ordenar la cancelación ni se ejecuta el pipeline en esos workers.
No prueban todavía la cancelación real de tareas Airflow ni procesos en el
contenedor. La prueba integrada posterior debe coordinar padre/descendientes
por IPC y demostrar que no siguen escribiendo después de cancelar la tarea.

## Límites de esta entrega

No se añade DAG, Compose, imagen nueva, workflow ni dependencia. El código del
adaptador aún no está incorporado a la imagen en ejecución; antes de usarlo
desde Airflow habrá que construir y validar el mismo código y referencias con
golden master, UID, ext4, umask y recuperación del sellado. Las alertas externas
siguen aplazadas a v1.1. Los servicios y almacenes existentes no se modifican.
