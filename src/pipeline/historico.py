"""CLI de inicialización, ingesta, reconstrucción y verificación histórica, sin publicación."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

from src.features.build_features import construir_dataset_analitico

from src.pipeline.validacion import (
    COLUMNAS_HECHOS,
    COLUMNAS_MACRO,
    MESES,
    comparar_filas,
    leer_hechos,
    leer_macro,
    macro_desde_largo,
    validar_decisiones,
    validar_features,
)
from src.pipeline.validacion_rebuild import (
    auditar_p75,
    comparar_features,
    leer_features_exactas,
    verificar_auditoria,
    verificar_etiquetas,
)


RAIZ_REPOSITORIO = Path(__file__).resolve().parents[2]
MANIFIESTO_REFERENCIAS = "config/referencias_historico.json"
COMMIT_REFERENCIAS = "3a07619c5c2f790f68ad8cddb0443eafa997d80d"
SBS = "data/processed/sbs_consolidado_2023_2025.csv"
BCRP = "data/raw/BCRP/bcrp_macro_2023_2025.csv"
MACRO = "data/raw/BCRP/macro_bcrp_2023_2025.csv"
FEATURES = "data/processed/dataset_ml_features.csv"
DECISIONES = "data/processed/ejemplo_decisiones_test_2025.csv"
REFERENCIAS = (SBS, BCRP, MACRO, FEATURES, DECISIONES)
CANDIDATO = Path("work/initial")
RECOMPUESTOS = {
    SBS: "consolidated/sbs_consolidado_2023_2025.csv",
    BCRP: "bcrp/bcrp_macro_2023_2025.csv",
    MACRO: "bcrp/macro_bcrp_2023_2025.csv",
}
FEATURES_RECONSTRUIDAS = "features/dataset_ml_features.csv"
AUDITORIA_P75 = "audit/p75.json"


def sha256(contenido):
    return hashlib.sha256(contenido).hexdigest()


def _leer_referencias(raiz):
    manifiesto_bytes = (raiz / MANIFIESTO_REFERENCIAS).read_bytes()
    manifiesto = json.loads(manifiesto_bytes)
    if not isinstance(manifiesto, dict) or set(manifiesto) != {"schema_version", "source_commit", "references"}:
        raise ValueError("Esquema incorrecto del manifiesto de referencias.")
    if manifiesto["schema_version"] != 1 or manifiesto["source_commit"] != COMMIT_REFERENCIAS:
        raise ValueError("Versión o commit de referencias no aprobado.")
    entradas = manifiesto["references"]
    if not isinstance(entradas, list) or len(entradas) != len(REFERENCIAS):
        raise ValueError("El manifiesto debe identificar las cinco referencias.")
    hashes = {}
    for entrada in entradas:
        if not isinstance(entrada, dict) or set(entrada) != {"path", "sha256"}:
            raise ValueError("Entrada de referencia incorrecta.")
        ruta, huella = entrada["path"], entrada["sha256"]
        if ruta not in REFERENCIAS or ruta in hashes:
            raise ValueError(f"Ruta de referencia no aprobada o duplicada: {ruta}.")
        if not isinstance(huella, str) or not re.fullmatch(r"[0-9a-f]{64}", huella):
            raise ValueError(f"SHA-256 incorrecto para {ruta}.")
        hashes[ruta] = huella
    contenidos = {}
    for ruta, huella in hashes.items():
        archivo = raiz / ruta
        if not archivo.resolve().is_relative_to(raiz):
            raise ValueError(f"Referencia fuera del repositorio: {ruta}.")
        contenido = archivo.read_bytes()
        if sha256(contenido) != huella:
            raise ValueError(f"Referencia alterada (SHA-256): {ruta}.")
        contenidos[ruta] = contenido
    return manifiesto_bytes, contenidos


def _destino(store, raiz):
    store = Path(store).resolve()
    if store.is_relative_to(raiz) or raiz.is_relative_to(store):
        raise ValueError("--store debe estar fuera del repositorio y no contenerlo.")
    return store


def _ruta_candidato(store, identificador):
    if not isinstance(identificador, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", identificador):
        raise ValueError("Identificador de candidato inválido: use letras ASCII, números, guiones o guiones bajos.")
    work = store / "work"
    candidato = work / identificador
    if work.is_symlink() or candidato.is_symlink() or not candidato.resolve().is_relative_to(work.resolve()):
        raise ValueError("El candidato debe permanecer dentro de work/ sin enlaces simbólicos.")
    return candidato


def _escribir_csv(ruta, columnas, filas):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("x", encoding="utf-8", newline="") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=columnas, lineterminator="\n")
        escritor.writeheader()
        escritor.writerows(filas)


def _fuentes(contenidos):
    sbs = leer_hechos(contenidos[SBS], "SBS")
    bcrp = leer_hechos(contenidos[BCRP], "BCRP")
    macro = leer_macro(contenidos[MACRO])
    comparar_filas(macro, macro_desde_largo(bcrp), ("fecha_id",), "BCRP ancho/largo")
    validar_features(contenidos[FEATURES])
    validar_decisiones(contenidos[DECISIONES])
    return {SBS: sbs, BCRP: bcrp, MACRO: macro}


def _artefactos_esperados():
    return (
        {f"references/{ruta}" for ruta in REFERENCIAS}
        | {f"partitions/{mes}/{fuente}.csv" for mes in MESES for fuente in ("sbs", "bcrp")}
        | set(RECOMPUESTOS.values())
    )


def _verificar_contenido(candidato, contenidos, adicionales=()):
    if candidato.is_symlink() or candidato.parent.is_symlink():
        raise ValueError("El candidato no puede ser un enlace simbólico.")
    archivos = set()
    for ruta in candidato.rglob("*"):
        if ruta.is_symlink():
            raise ValueError(f"Enlace simbólico no permitido en el candidato: {ruta.name}.")
        if ruta.is_file():
            archivos.add(ruta.relative_to(candidato).as_posix())
    esperados = _artefactos_esperados() | set(adicionales)
    if archivos - {"manifest.json"} != esperados:
        raise ValueError("Candidato incompleto o con artefactos inesperados.")
    for ruta, contenido in contenidos.items():
        if (candidato / "references" / ruta).read_bytes() != contenido:
            raise ValueError(f"Copia de referencia alterada: {ruta}.")
    fuentes = _fuentes(contenidos)
    for referencia, fuente in ((SBS, "SBS"), (BCRP, "BCRP")):
        filas = []
        for mes in MESES:
            contenido = (candidato / f"partitions/{mes}/{fuente.lower()}.csv").read_bytes()
            filas.extend(leer_hechos(contenido, fuente, meses=(mes,)))
        comparar_filas(fuentes[referencia], filas, COLUMNAS_HECHOS[:-1], f"Particiones {fuente}")
        recompuesto = leer_hechos((candidato / RECOMPUESTOS[referencia]).read_bytes(), fuente)
        comparar_filas(filas, recompuesto, COLUMNAS_HECHOS[:-1], f"Recomposición {fuente}")
    macro = leer_macro((candidato / RECOMPUESTOS[MACRO]).read_bytes())
    comparar_filas(fuentes[MACRO], macro, ("fecha_id",), "Recomposición BCRP ancho")
    return {
        ruta: sha256((candidato / ruta).read_bytes())
        for ruta in sorted(esperados)
    }


def verificar(store, raiz_repositorio=RAIZ_REPOSITORIO, candidato="initial"):
    """Comprueba un candidato preparado sin escribir ni reparar archivos."""
    raiz = Path(raiz_repositorio).resolve()
    store = _destino(store, raiz)
    manifiesto_bytes, contenidos = _leer_referencias(raiz)
    identificador = candidato
    candidato = _ruta_candidato(store, identificador)
    marcador = candidato / "manifest.json"
    if not marcador.is_file() or marcador.is_symlink():
        raise ValueError("Candidato incompleto: falta un manifiesto preparado válido.")
    preparado = json.loads(marcador.read_bytes())
    if not isinstance(preparado, dict):
        raise ValueError("Esquema incorrecto del manifiesto del candidato.")
    etapa = preparado.get("stage")
    if etapa == "historical-rebuild":
        return _verificar_reconstruccion(store, candidato, preparado, raiz, manifiesto_bytes, contenidos)
    if etapa == "historical-ingest":
        from src.pipeline.ingesta import verificar_ingesta
        return verificar_ingesta(store, candidato, preparado, raiz, manifiesto_bytes, contenidos)
    if etapa != "historical-init" or identificador != "initial":
        raise ValueError("Etapa o identificador de inicialización no reconocido.")
    esperado = {
        "schema_version": 1,
        "stage": "historical-init",
        "status": "prepared",
        "source_commit": COMMIT_REFERENCIAS,
        "references_manifest_sha256": sha256(manifiesto_bytes),
        "artifacts": _verificar_contenido(candidato, contenidos),
    }
    if preparado != esperado:
        raise ValueError("Manifiesto del candidato incorrecto o hashes de artefactos diferentes.")
    return preparado


def _manifiesto_rebuild(candidato, origen_id, origen_hash, manifiesto_bytes, contenidos, artefactos):
    return {
        "schema_version": 1, "stage": "historical-rebuild", "status": "prepared",
        "candidate": candidato.name, "source_commit": COMMIT_REFERENCIAS,
        "source_candidate": {"id": origen_id, "manifest_sha256": origen_hash},
        "references_manifest_sha256": sha256(manifiesto_bytes),
        "references": {ruta: sha256(contenidos[ruta]) for ruta in REFERENCIAS},
        "inputs": {ruta: {"path": RECOMPUESTOS[ruta], "sha256": artefactos[RECOMPUESTOS[ruta]]}
                   for ruta in (SBS, BCRP)},
        "results": {nombre: {"path": ruta, "sha256": artefactos[ruta]}
                    for nombre, ruta in (("features", FEATURES_RECONSTRUIDAS), ("p75", AUDITORIA_P75))},
        "artifacts": artefactos,
    }


def _verificar_resultados(candidato, contenidos):
    resultado = leer_features_exactas((candidato / FEATURES_RECONSTRUIDAS).read_bytes())
    comparar_features(resultado, contenidos[FEATURES])
    auditoria = json.loads((candidato / AUDITORIA_P75).read_bytes())
    verificar_auditoria(auditoria, contenidos[SBS])
    verificar_etiquetas(resultado, auditoria)


def _verificar_reconstruccion(store, candidato, preparado, raiz, manifiesto_bytes, contenidos):
    origen = preparado.get("source_candidate")
    if not isinstance(origen, dict) or set(origen) != {"id", "manifest_sha256"}:
        raise ValueError("Procedencia del candidato incorrecta.")
    # Esta etapa solo reconstruye la inicialización, sin cadenas de reconstrucción.
    if origen["id"] != "initial" or candidato.name == "initial":
        raise ValueError("La reconstrucción debe proceder del candidato initial.")
    verificar(store, raiz, candidato=origen["id"])
    origen_hash = sha256((_ruta_candidato(store, origen["id"]) / "manifest.json").read_bytes())
    artefactos = _verificar_contenido(candidato, contenidos, (FEATURES_RECONSTRUIDAS, AUDITORIA_P75))
    _verificar_resultados(candidato, contenidos)
    esperado = _manifiesto_rebuild(candidato, origen["id"], origen_hash, manifiesto_bytes, contenidos, artefactos)
    if preparado != esperado:
        raise ValueError("Manifiesto de reconstrucción incorrecto o hashes de artefactos diferentes.")
    return preparado


def reconstruir(store, candidato="initial", raiz_repositorio=RAIZ_REPOSITORIO):
    """Reconstruye en un candidato nuevo; un fallo deja archivos sin manifiesto final."""
    raiz = Path(raiz_repositorio).resolve()
    store = _destino(store, raiz)
    origen = _ruta_candidato(store, candidato)
    preparado = verificar(store, raiz, candidato=candidato)
    if preparado["stage"] != "historical-init":
        raise ValueError("Esta etapa requiere un candidato de inicialización.")
    manifiesto_bytes, contenidos = _leer_referencias(raiz)
    origen_bytes = (origen / "manifest.json").read_bytes()
    if json.loads(origen_bytes) != preparado:
        raise ValueError("El manifiesto de origen cambió después de verificarlo.")
    nuevo = _ruta_candidato(store, f"rebuild-{uuid4().hex}")
    nuevo.mkdir(exist_ok=False)
    for ruta, huella in preparado["artifacts"].items():
        contenido = (origen / ruta).read_bytes()
        if sha256(contenido) != huella:
            raise ValueError(f"El candidato de origen cambió durante la copia: {ruta}.")
        destino = nuevo / ruta
        destino.parent.mkdir(parents=True, exist_ok=True)
        with destino.open("xb") as archivo:
            archivo.write(contenido)
    _verificar_contenido(nuevo, contenidos)
    entradas = [nuevo / RECOMPUESTOS[ruta] for ruta in (SBS, BCRP)]
    for ruta in entradas:
        if not ruta.is_file():
            raise ValueError(f"Entrada requerida antes del constructor: {ruta}.")
    capturas = []

    def observar(datos):
        if capturas:
            raise ValueError("El constructor invocó el observador P75 más de una vez.")
        capturas.append(auditar_p75(datos, contenidos[SBS]))

    resultado = construir_dataset_analitico(
        ruta_sbs=entradas[0], ruta_bcrp=entradas[1],
        ruta_salida=nuevo / FEATURES_RECONSTRUIDAS, observador_p75=observar,
    )
    if len(capturas) != 1:
        raise ValueError("El constructor no entregó la observación P75 requerida.")
    comparar_features(resultado, contenidos[FEATURES])
    verificar_etiquetas(resultado, capturas[0])
    auditoria = nuevo / AUDITORIA_P75
    auditoria.parent.mkdir(parents=True, exist_ok=True)
    with auditoria.open("x", encoding="utf-8") as archivo:
        json.dump(capturas[0], archivo, indent=2, ensure_ascii=False, allow_nan=False)
        archivo.write("\n")
    _verificar_resultados(nuevo, contenidos)
    artefactos = _verificar_contenido(nuevo, contenidos, (FEATURES_RECONSTRUIDAS, AUDITORIA_P75))
    verificar(store, raiz, candidato=candidato)
    if (origen / "manifest.json").read_bytes() != origen_bytes or _leer_referencias(raiz)[0] != manifiesto_bytes:
        raise ValueError("Las referencias o el manifiesto de origen cambiaron durante la reconstrucción.")
    final = _manifiesto_rebuild(nuevo, candidato, sha256(origen_bytes), manifiesto_bytes, contenidos, artefactos)
    with (nuevo / "manifest.json").open("x", encoding="utf-8") as archivo:
        json.dump(final, archivo, indent=2, ensure_ascii=False, allow_nan=False)
        archivo.write("\n")
    return nuevo


def inicializar(store, raiz_repositorio=RAIZ_REPOSITORIO):
    """Prepara una sola inicialización; un almacén existente nunca se sobrescribe."""
    raiz = Path(raiz_repositorio).resolve()
    store = _destino(store, raiz)
    manifiesto_bytes, contenidos = _leer_referencias(raiz)
    fuentes = _fuentes(contenidos)
    if store.exists():
        verificar(store, raiz)
        raise ValueError("El almacén ya está preparado; no se sobrescribe.")
    store.mkdir(parents=True, exist_ok=False)
    candidato = store / CANDIDATO
    candidato.mkdir(parents=True)
    for ruta, contenido in contenidos.items():
        destino = candidato / "references" / ruta
        destino.parent.mkdir(parents=True, exist_ok=True)
        with destino.open("xb") as archivo:
            archivo.write(contenido)
    for mes in MESES:
        for referencia, fuente in ((SBS, "sbs"), (BCRP, "bcrp")):
            filas = [fila for fila in fuentes[referencia] if fila["fecha_id"] == mes]
            _escribir_csv(candidato / f"partitions/{mes}/{fuente}.csv", COLUMNAS_HECHOS, filas)
    recompuestos = {}
    for referencia, fuente in ((SBS, "SBS"), (BCRP, "BCRP")):
        filas = []
        for mes in MESES:
            contenido = (candidato / f"partitions/{mes}/{fuente.lower()}.csv").read_bytes()
            filas.extend(leer_hechos(contenido, fuente, meses=(mes,)))
        recompuestos[referencia] = filas
        _escribir_csv(candidato / RECOMPUESTOS[referencia], COLUMNAS_HECHOS, filas)
    _escribir_csv(
        candidato / RECOMPUESTOS[MACRO], COLUMNAS_MACRO,
        macro_desde_largo(recompuestos[BCRP])
    )
    artefactos = _verificar_contenido(candidato, contenidos)
    if _leer_referencias(raiz)[0] != manifiesto_bytes:
        raise ValueError("El manifiesto de referencias cambió durante la preparación.")
    preparado = {
        "schema_version": 1,
        "stage": "historical-init",
        "status": "prepared",
        "source_commit": COMMIT_REFERENCIAS,
        "references_manifest_sha256": sha256(manifiesto_bytes),
        "artifacts": artefactos,
    }
    with (candidato / "manifest.json").open("x", encoding="utf-8") as archivo:
        json.dump(preparado, archivo, indent=2, ensure_ascii=False)
        archivo.write("\n")
    return candidato


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    comandos = parser.add_subparsers(dest="comando", required=True)
    for nombre in ("init", "verify", "rebuild", "ingest"):
        comando = comandos.add_parser(nombre)
        comando.add_argument("--store", required=True, type=Path)
        if nombre == "verify":
            comando.add_argument("--candidate", default="initial")
        elif nombre in ("rebuild", "ingest"):
            comando.add_argument("--candidate", required=True)
        if nombre == "ingest":
            comando.add_argument("--month", required=True)
            comando.add_argument("--sbs-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.comando == "init":
            candidato = inicializar(args.store)
            print(f"Candidato preparado: {candidato}")
        elif args.comando == "rebuild":
            candidato = reconstruir(args.store, candidato=args.candidate)
            print(f"Candidato reconstruido: {candidato.name}\nRuta: {candidato}")
        elif args.comando == "ingest":
            from src.pipeline.ingesta import ingerir
            candidato = ingerir(args.store, args.candidate, args.month, args.sbs_dir)
            print(f"Fuentes ingeridas y validadas: {candidato.name}\nRuta: {candidato}")
        else:
            verificar(args.store, candidato=args.candidate)
            print("Candidato verificado: 36 particiones, referencias y recomposición correctas.")
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
