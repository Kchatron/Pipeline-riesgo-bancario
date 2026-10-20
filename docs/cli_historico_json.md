# Resultados JSON de la CLI histórica

Etapa 1 de preparación de Airflow. `--json` se admite después del nombre de
cualquiera de los seis comandos existentes. Ejemplo:

```bash
python -m src.pipeline.historico rebuild --store RUTA --candidate initial --json
```

En una operación exitosa (código 0), stdout contiene exactamente un documento
JSON UTF-8 terminado en salto de línea. Los mensajes de progreso van a stderr.
El consumidor debe comprobar primero el código de salida y después decodificar
stdout completo, sin buscar líneas ni interpretar mensajes humanos.

Sin `--json` se mantiene la salida anterior, incluidos los JSON históricos de
`publish` y `resolve`, que no adquieren campos nuevos en ese modo.

## Contratos, esquema 1

Todos los resultados incluyen `schema_version: 1`, `operation` con el nombre del
comando y `status`. El esquema del resultado es independiente del esquema del
manifiesto del candidato o paquete.

| Operación | Campos adicionales | Estado |
|---|---|---|
| `init` | `candidate_id`, `path`, `manifest_sha256`, `stage` | `prepared` |
| `ingest` | `candidate_id`, `path`, `manifest_sha256`, `stage`, `month`, `source_candidate` | `sources-validated` |
| `rebuild` | `candidate_id`, `path`, `manifest_sha256`, `stage`, `source_candidate` | `prepared` |
| `verify --candidate ID` | `candidate_id`, `path`, `manifest_sha256`, `stage`; `month` y `source_candidate` cuando existen en ese manifiesto | Estado del candidato verificado |
| `verify --version ID` | `version_id`, `path`, `manifest_sha256`, `stage` | `complete` |
| `publish --candidate ID` o `publish --resume-version ID` | `version_id`, `path`, `manifest_sha256`, `already_current` | `published` o `already-current` |
| `resolve` | `version_id`, `manifest_sha256`, `root`, `sbs`, `bcrp_largo`, `bcrp_ancho`, `features` | `resolved` |

`verify` sin selector sigue verificando `initial`. Los selectores incompatibles
conservan su exclusión mutua. Las rutas son absolutas. Los identificadores
siguen siendo los del pipeline: `initial`, `ingest-UUID`, `rebuild-UUID` y `v-HASH`.

`source_candidate` contiene el identificador y hash del manifiesto del origen
directo, con las claves existentes `id` y `manifest_sha256`.

En resultados de candidatos y de `verify --version`, `manifest_sha256` se calcula
sobre los bytes del archivo, no sobre una nueva serialización JSON. La respuesta
de `publish` utiliza directamente la identidad devuelta por el publicador;
`resolve` utiliza el objeto inmutable devuelto por el lector. No se vuelve a
consultar LATEST para sustituir una identidad por otra más reciente.

Ejemplo de reconstrucción (identificadores y hashes abreviados):

```json
{
  "schema_version": 1,
  "operation": "rebuild",
  "candidate_id": "rebuild-...",
  "path": "/almacen/work/rebuild-...",
  "manifest_sha256": "...",
  "stage": "historical-rebuild",
  "status": "prepared",
  "source_candidate": {"id": "initial", "manifest_sha256": "..."}
}
```

## Errores y evidencia

Un fallo no emite un resultado de éxito. Con `--json`, los errores gestionados
dejan stdout vacío y conservan mensajes, notas y rutas de evidencia en stderr.
No se añade un contrato JSON de errores ni se alteran las operaciones de datos.

| Código | Significado conservado |
|---|---|
| 0 | Éxito o publicación ya actual |
| 1 | Validación u otro error gestionado; en publicación, previo al reemplazo |
| 2 | Argumentos inválidos, gestionados por argparse |
| 3 | Base de publicación obsoleta |
| 4 | Publicación visible o resultado incierto, con durabilidad sin confirmar |

El código 4 no indica que LATEST esté intacto. Hay que inspeccionar la evidencia
y aplicar la reanudación explícita existente. La salida JSON no cambia ese
protocolo, ni modifica manifiestos, candidatos, versiones o inventarios.

## Pendientes para las etapas posteriores

- Comprobar umask y recuperación dentro del contenedor, con el UID y filesystem
  reales de ejecución, incluido el sellado de la raíz pendiente.
- Ejecutar exactamente el mismo código y referencias que pasaron el golden
  master; registrar su identidad y la de la imagen y dependencias.
- Definir y probar la terminación del subproceso y sus descendientes al cancelar
  una tarea; comprobar que no continúan modificando el almacén tras la cancelación.
- Las alertas externas quedan aplazadas a v1.1.

Esta etapa no crea imagen, dependencias, DAG ni configuración de Compose.
