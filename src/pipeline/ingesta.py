"""Ingesta retrospectiva privada: fuentes mensuales y evidencia, sin publicación."""

import json
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pandas as pd

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h
from src.pipeline.contexto_publicacion import CONTEXTO, campos_contexto, capturar_contexto, guardar_contexto
from src.pipeline.validacion import (
    COLUMNAS_HECHOS, COLUMNAS_MACRO, MESES, comparar_filas,
    leer_hechos, leer_macro, macro_desde_largo,
)
from src.pipeline.validacion_ingest import comparar_mes, leer_mes, validar_mes


INFORME = "audit/differences.json"
PROCEDENCIA = "audit/inputs.json"
SBS_MES = "extracted/sbs.csv"
BCRP_MES = "extracted/bcrp/bcrp_macro_2023_2025.csv"
MACRO_MES = "extracted/bcrp/macro_bcrp_2023_2025.csv"
REPORTES = ("B-2362", "B-2334", "Tasas")
FUENTES = (h.SBS, h.BCRP, h.MACRO)


def _json(ruta, valor):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", encoding="utf-8") as archivo:
        json.dump(valor, archivo, indent=2, ensure_ascii=False, allow_nan=False)
        archivo.write("\n")


def _copiar(ruta, contenido):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("xb") as archivo:
        archivo.write(contenido)


def resolver_excel(carpeta, mes):
    """No utiliza el resolver histórico que selecciona la primera coincidencia."""
    carpeta = Path(carpeta)
    archivos = []
    for prefijo in REPORTES:
        coincidencias = [ruta for ruta in carpeta.iterdir()
                        if ruta.stem == f"{prefijo}-{mes}" and ruta.suffix.lower() in (".xls", ".xlsx")]
        if len(coincidencias) != 1 or not coincidencias[0].is_file():
            raise ValueError(f"Se requiere exactamente un Excel SBS {prefijo}-{mes}; encontrados: {len(coincidencias)}.")
        archivos.append(coincidencias[0])
    return archivos


def _peticion(mes):
    codigos = "-".join(bcrp_extractor.SERIES_BCRP)
    return {"start": mes, "end": mes, "series": bcrp_extractor.SERIES_BCRP,
            "url": f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{codigos}/json/{mes}/{mes}",
            "outputs": [BCRP_MES, MACRO_MES]}


def _inventario(candidato):
    if candidato.is_symlink() or candidato.parent.is_symlink():
        raise ValueError("Enlace simbólico no permitido en candidato de ingesta.")
    artefactos = {}
    for ruta in candidato.rglob("*"):
        if ruta.is_symlink():
            raise ValueError("Enlace simbólico no permitido en candidato de ingesta.")
        if ruta.is_file() and ruta.name != "manifest.json":
            artefactos[ruta.relative_to(candidato).as_posix()] = h.sha256(ruta.read_bytes())
        elif ruta.is_file() and ruta != candidato / "manifest.json":
            raise ValueError("Manifiesto anidado inesperado en ingesta.")
    return dict(sorted(artefactos.items()))


def _validar_procedencia(candidato, entradas, mes):
    if not isinstance(entradas, dict) or set(entradas) != {"schema_version", "month", "sbs", "bcrp"}:
        raise ValueError("Esquema de procedencia de ingesta incorrecto.")
    if entradas["schema_version"] != 1 or entradas["month"] != mes or entradas["bcrp"] != _peticion(mes):
        raise ValueError("Procedencia BCRP o periodo de ingesta incorrectos.")
    sbs = entradas["sbs"]
    if not isinstance(sbs, list) or len(sbs) != 3:
        raise ValueError("Procedencia SBS incompleta.")
    rutas = set()
    for prefijo, entrada in zip(REPORTES, sbs):
        if not isinstance(entrada, dict) or set(entrada) != {"report", "source_uri", "path", "sha256"}:
            raise ValueError("Esquema de entrada Excel SBS incorrecto.")
        ruta = entrada["path"]
        if not isinstance(ruta, str):
            raise ValueError("Ruta de Excel SBS incorrecta.")
        nombre = Path(ruta)
        if (entrada["report"] != prefijo or nombre.parent.as_posix() != "inputs/sbs"
                or nombre.stem != f"{prefijo}-{mes}" or nombre.suffix.lower() not in (".xls", ".xlsx")):
            raise ValueError("Ruta o reporte de Excel SBS incorrectos.")
        uri = entrada["source_uri"]
        if not isinstance(uri, str) or urlparse(uri).scheme != "file" or not urlparse(uri).path.startswith("/"):
            raise ValueError("Procedencia original del Excel SBS incorrecta.")
        if h.sha256((candidato / ruta).read_bytes()) != entrada["sha256"]:
            raise ValueError("Hash de Excel SBS diferente de su procedencia.")
        rutas.add(ruta)
    return rutas


def _comparaciones(candidato, origen, contenidos, mes):
    sbs, bcrp, ancho = leer_mes(
        (candidato / SBS_MES).read_bytes(), (candidato / BCRP_MES).read_bytes(),
        (candidato / MACRO_MES).read_bytes(), mes,
    )
    referencias = {"SBS": leer_hechos(contenidos[h.SBS], "SBS"),
                   "BCRP": leer_hechos(contenidos[h.BCRP], "BCRP")}
    inicial = {fuente: leer_hechos((origen / h.RECOMPUESTOS[ruta]).read_bytes(), fuente)
               for fuente, ruta in (("SBS", h.SBS), ("BCRP", h.BCRP))}
    return comparar_mes(mes, sbs, bcrp, ancho, referencias, inicial)


def _particiones(candidato, origen, mes):
    fuentes = {}
    for nombre, extraido in (("SBS", SBS_MES), ("BCRP", BCRP_MES)):
        filas = []
        for periodo in MESES:
            ruta = f"partitions/{periodo}/{nombre.lower()}.csv"
            contenido = (candidato / ruta).read_bytes()
            esperado = (candidato / extraido).read_bytes() if periodo == mes else (origen / ruta).read_bytes()
            if contenido != esperado:
                raise ValueError(f"Partición modificada o distinta de su extracción: {ruta}.")
            filas.extend(leer_hechos(contenido, nombre, meses=(periodo,)))
        fuentes[nombre] = filas
    return fuentes


def _validar_contenido(candidato, origen, contenidos, mes, esquema=1, adicionales=()):
    inventario = _inventario(candidato)
    entradas = json.loads((candidato / PROCEDENCIA).read_bytes())
    excel = _validar_procedencia(candidato, entradas, mes)
    esperados = (
        {f"references/{ruta}" for ruta in FUENTES}
        | {f"partitions/{periodo}/{fuente}.csv" for periodo in MESES for fuente in ("sbs", "bcrp")}
        | set(h.RECOMPUESTOS.values()) | excel | {INFORME, PROCEDENCIA, SBS_MES, BCRP_MES, MACRO_MES}
    )
    campos_contexto(candidato, esquema)
    if esquema == 2:
        esperados.add(CONTEXTO)
    esperados.update(adicionales)
    if set(inventario) != esperados:
        raise ValueError("Inventario de ingesta incompleto o con artefactos inesperados.")
    for ruta in FUENTES:
        if (candidato / "references" / ruta).read_bytes() != contenidos[ruta]:
            raise ValueError(f"Referencia de ingesta alterada: {ruta}.")
    informe = _comparaciones(candidato, origen, contenidos, mes)
    guardado = json.loads((candidato / INFORME).read_bytes())
    if informe != guardado:
        raise ValueError("Informe de diferencias alterado o incompatible con las entradas.")
    if informe["status"] != "matched":
        raise ValueError("Ingesta bloqueada hasta decisión humana: discrepancias numéricas.")
    fuentes = _particiones(candidato, origen, mes)
    for nombre, ruta in (("SBS", h.SBS), ("BCRP", h.BCRP)):
        recompuesto = leer_hechos((candidato / h.RECOMPUESTOS[ruta]).read_bytes(), nombre)
        # Aquí se exige conservar tokens entre particiones y recomposición, no frente a referencias.
        comparar_filas(fuentes[nombre], recompuesto, COLUMNAS_HECHOS[:-1], f"Recomposición ingerida {nombre}")
    macro = leer_macro((candidato / h.RECOMPUESTOS[h.MACRO]).read_bytes())
    comparar_filas(macro_desde_largo(fuentes["BCRP"]), macro, ("fecha_id",), "Recomposición macro ingerida")
    return inventario, entradas


def _manifiesto(candidato, mes, origen_bytes, referencias_bytes, contenidos, inventario, entradas, esquema=1, identificador=None):
    return {
        "schema_version": esquema, "stage": "historical-ingest", "status": "sources-validated",
        "candidate": candidato.name if identificador is None else identificador, "month": mes,
        "source_candidate": {"id": "initial", "manifest_sha256": h.sha256(origen_bytes)},
        "source_commit": h.COMMIT_REFERENCIAS, "references_manifest_sha256": h.sha256(referencias_bytes),
        "references": {ruta: h.sha256(contenidos[ruta]) for ruta in h.REFERENCIAS},
        "inputs": entradas, "comparison_report": {"path": INFORME, "sha256": inventario[INFORME]},
        "results": {ruta: {"path": h.RECOMPUESTOS[ruta], "sha256": inventario[h.RECOMPUESTOS[ruta]]}
                    for ruta in FUENTES},
        "artifacts": inventario,
        **campos_contexto(candidato, esquema),
    }


def verificar_ingesta(store, candidato, preparado, raiz, referencias_bytes, contenidos):
    mes = preparado.get("month")
    validar_mes(mes)
    origen_meta = preparado.get("source_candidate")
    if (candidato.name == "initial" or not isinstance(origen_meta, dict)
            or set(origen_meta) != {"id", "manifest_sha256"} or origen_meta["id"] != "initial"):
        raise ValueError("La ingesta solo puede proceder de initial.")
    h.verificar(store, raiz, candidato="initial")
    origen = h._ruta_candidato(store, "initial")
    return verificar_ingesta_archivos(candidato, preparado, origen, (origen / "manifest.json").read_bytes(),
                                     referencias_bytes, contenidos)


def verificar_ingesta_archivos(candidato, preparado, origen, origen_bytes, referencias_bytes, contenidos,
                               identificador=None, adicionales=()):
    mes = preparado.get("month")
    validar_mes(mes)
    esquema = preparado.get("schema_version")
    inventario, entradas = _validar_contenido(candidato, origen, contenidos, mes, esquema, adicionales)
    inventario = {ruta: huella for ruta, huella in inventario.items() if ruta not in adicionales}
    esperado = _manifiesto(candidato, mes, origen_bytes, referencias_bytes, contenidos, inventario, entradas,
                           esquema, identificador)
    if preparado != esperado:
        raise ValueError("Manifiesto de ingesta incorrecto o hashes diferentes.")
    return preparado


def ingerir(store, candidato, mes, sbs_dir, raiz_repositorio=h.RAIZ_REPOSITORIO):
    validar_mes(mes)
    if candidato != "initial":
        raise ValueError("La ingesta admite únicamente --candidate initial.")
    raiz = Path(raiz_repositorio).resolve()
    store = h._destino(store, raiz)
    preparado = h.verificar(store, raiz, candidato=candidato)
    origen = h._ruta_candidato(store, candidato)
    origen_bytes = (origen / "manifest.json").read_bytes()
    if json.loads(origen_bytes) != preparado:
        raise ValueError("El manifiesto initial cambió después de su verificación.")
    referencias_bytes, contenidos = h._leer_referencias(raiz)
    excel = resolver_excel(sbs_dir, mes)
    contexto = capturar_contexto(store, raiz / h.MANIFIESTO_REFERENCIAS)
    nuevo = h._ruta_candidato(store, f"ingest-{uuid4().hex}")
    nuevo.mkdir(exist_ok=False)
    try:
        guardar_contexto(nuevo, contexto)
        for ruta in FUENTES:
            _copiar(nuevo / "references" / ruta, contenidos[ruta])
        for periodo in MESES:
            if periodo != mes:
                for fuente in ("sbs", "bcrp"):
                    ruta = f"partitions/{periodo}/{fuente}.csv"
                    contenido = (origen / ruta).read_bytes()
                    if h.sha256(contenido) != preparado["artifacts"][ruta]:
                        raise ValueError("Una partición de initial cambió durante la copia.")
                    _copiar(nuevo / ruta, contenido)
        entradas = {"schema_version": 1, "month": mes, "sbs": [], "bcrp": _peticion(mes)}
        for prefijo, archivo in zip(REPORTES, excel):
            contenido = archivo.read_bytes()
            ruta = f"inputs/sbs/{archivo.name}"
            _copiar(nuevo / ruta, contenido)
            entradas["sbs"].append({"report": prefijo, "source_uri": archivo.resolve().as_uri(),
                                     "path": ruta, "sha256": h.sha256(contenido)})
        _json(nuevo / PROCEDENCIA, entradas)
        funciones = (sbs_extractor.extraer_morosidad, sbs_extractor.extraer_creditos_directos,
                     sbs_extractor.extraer_tasas_activas)
        resultados = [funcion(nuevo / entrada["path"], mes)
                      for funcion, entrada in zip(funciones, entradas["sbs"])]
        if any(tuple(resultado.columns) != COLUMNAS_HECHOS for resultado in resultados):
            raise ValueError("Esquema de extracción SBS incorrecto.")
        sbs = pd.concat(resultados, ignore_index=True)
        (nuevo / SBS_MES).parent.mkdir(parents=True, exist_ok=True)
        sbs.to_csv(nuevo / SBS_MES, index=False)
        leer_hechos((nuevo / SBS_MES).read_bytes(), "SBS", meses=(mes,))
        bcrp_extractor.extraer_bcrp_api(periodo_ini=mes, periodo_fin=mes,
                                        directorio_salida=nuevo / "extracted/bcrp")
        informe = _comparaciones(nuevo, origen, contenidos, mes)
        _json(nuevo / INFORME, informe)
        if informe["status"] != "matched":
            raise ValueError("Bloqueado hasta decisión humana: discrepancias; no se acepta automáticamente.")
        for fuente, ruta in (("sbs", SBS_MES), ("bcrp", BCRP_MES)):
            _copiar(nuevo / f"partitions/{mes}/{fuente}.csv", (nuevo / ruta).read_bytes())
        fuentes = _particiones(nuevo, origen, mes)
        for nombre, ruta in (("SBS", h.SBS), ("BCRP", h.BCRP)):
            h._escribir_csv(nuevo / h.RECOMPUESTOS[ruta], COLUMNAS_HECHOS, fuentes[nombre])
        h._escribir_csv(nuevo / h.RECOMPUESTOS[h.MACRO], COLUMNAS_MACRO, macro_desde_largo(fuentes["BCRP"]))
        inventario, entradas = _validar_contenido(nuevo, origen, contenidos, mes, esquema=2)
        h.verificar(store, raiz, candidato="initial")
        if (origen / "manifest.json").read_bytes() != origen_bytes or h._leer_referencias(raiz)[0] != referencias_bytes:
            raise ValueError("Origen o referencias cambiaron durante la ingesta.")
        final = _manifiesto(nuevo, mes, origen_bytes, referencias_bytes, contenidos, inventario, entradas, esquema=2)
        with (nuevo / "manifest.json").open("x", encoding="utf-8") as archivo:
            json.dump(final, archivo, indent=2, ensure_ascii=False, allow_nan=False)
            archivo.write("\n")
        return nuevo
    except Exception as error:
        ruta = nuevo / INFORME
        informe = json.loads(ruta.read_bytes()) if ruta.is_file() else {
            "schema_version": 1, "stage": "historical-ingest", "month": mes,
            "status": "failed", "comparisons": [],
        }
        if informe["status"] != "blocked":
            informe["status"] = "failed"
        informe["error"] = str(error)
        _json(ruta, informe)
        raise ValueError(f"Ingesta fallida o bloqueada: {error} Evidencia conservada en {nuevo}.") from error
