"""CLI de inicialización, ingesta, reconstrucción y verificación histórica, sin publicación."""

import argparse
from contextlib import nullcontext, redirect_stdout
import csv
import hashlib
import json
from pathlib import Path
import re
import sys
from uuid import uuid4

from src.features.build_features import construir_dataset_analitico
from src.pipeline.contexto_publicacion import CONTEXTO, campos_contexto, capturar_contexto, guardar_contexto

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
    verificar_cortes_referencia,
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
DIAGNOSTICO_REBUILD = "audit/rebuild-failure.json"


def sha256(contenido):
    return hashlib.sha256(contenido).hexdigest()


def _hashes_referencias(manifiesto_bytes):
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
    return hashes


def _leer_referencias(raiz):
    manifiesto_bytes = (raiz / MANIFIESTO_REFERENCIAS).read_bytes()
    hashes = _hashes_referencias(manifiesto_bytes)
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
    return _verificar_initial_archivos(candidato, preparado, manifiesto_bytes, contenidos)


def _verificar_initial_archivos(candidato, preparado, manifiesto_bytes, contenidos):
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


def _manifiesto_rebuild(candidato, origen_id, origen_hash, manifiesto_bytes, contenidos, artefactos, esquema=1, identificador=None):
    return {
        "schema_version": esquema, "stage": "historical-rebuild", "status": "prepared",
        "candidate": candidato.name if identificador is None else identificador, "source_commit": COMMIT_REFERENCIAS,
        "source_candidate": {"id": origen_id, "manifest_sha256": origen_hash},
        "references_manifest_sha256": sha256(manifiesto_bytes),
        "references": {ruta: sha256(contenidos[ruta]) for ruta in REFERENCIAS},
        "inputs": {ruta: {"path": RECOMPUESTOS[ruta], "sha256": artefactos[RECOMPUESTOS[ruta]]}
                   for ruta in (SBS, BCRP)},
        "results": {nombre: {"path": ruta, "sha256": artefactos[ruta]}
                    for nombre, ruta in (("features", FEATURES_RECONSTRUIDAS), ("p75", AUDITORIA_P75))},
        "artifacts": artefactos,
        **campos_contexto(candidato, esquema),
    }


def _verificar_resultados(candidato, contenidos):
    resultado = leer_features_exactas((candidato / FEATURES_RECONSTRUIDAS).read_bytes())
    comparar_features(resultado, contenidos[FEATURES])
    auditoria = json.loads((candidato / AUDITORIA_P75).read_bytes())
    verificar_auditoria(auditoria, (candidato / RECOMPUESTOS[SBS]).read_bytes())
    verificar_cortes_referencia(auditoria, contenidos[SBS])
    verificar_etiquetas(resultado, auditoria)


def _origen_rebuild(store, identificador, raiz):
    origen = _ruta_candidato(store, identificador)
    marcador = origen / "manifest.json"
    if not marcador.is_file() or marcador.is_symlink():
        raise ValueError("Candidato incompleto: falta un manifiesto preparado válido.")
    datos = json.loads(marcador.read_bytes())
    # Se inspecciona la etapa antes de recursar: un rebuild nunca es un origen admitido.
    if not isinstance(datos, dict) or (datos.get("stage"), datos.get("status")) not in (
        ("historical-init", "prepared"), ("historical-ingest", "sources-validated")
    ):
        raise ValueError("La reconstrucción requiere initial o una ingesta sources-validated; no admite cadenas de reconstrucción.")
    return origen, verificar(store, raiz, candidato=identificador)


def _verificar_copia_rebuild(candidato, preparado, contenidos, con_resultados=False, esquema=1):
    adicionales = (FEATURES_RECONSTRUIDAS, AUDITORIA_P75) if con_resultados else ()
    campos_contexto(candidato, esquema)
    if preparado["stage"] == "historical-init":
        if esquema == 2:
            adicionales = (*adicionales, CONTEXTO)
        return _verificar_contenido(candidato, contenidos, adicionales)
    if esquema != preparado["schema_version"]:
        raise ValueError("El esquema y contexto deben heredarse de la ingesta, sin asignación retrospectiva.")
    from src.pipeline.ingesta import _inventario
    inventario = _inventario(candidato)
    if set(inventario) != set(preparado["artifacts"]) | set(adicionales):
        raise ValueError("Inventario de reconstrucción desde ingesta incompleto o inesperado.")
    for ruta, huella in preparado["artifacts"].items():
        if inventario[ruta] != huella:
            raise ValueError(f"Fuente o evidencia copiada diferente del origen verificado: {ruta}.")
    return inventario


def _verificar_reconstruccion(store, candidato, preparado, raiz, manifiesto_bytes, contenidos):
    origen = preparado.get("source_candidate")
    if not isinstance(origen, dict) or set(origen) != {"id", "manifest_sha256"}:
        raise ValueError("Procedencia del candidato incorrecta.")
    if candidato.name == "initial" or origen["id"] == candidato.name:
        raise ValueError("La reconstrucción debe proceder de initial o de una ingesta validada.")
    ruta_origen, origen_preparado = _origen_rebuild(store, origen["id"], raiz)
    return _verificar_rebuild_archivos(candidato, preparado, origen_preparado,
                                      (ruta_origen / "manifest.json").read_bytes(), manifiesto_bytes, contenidos)


def _verificar_rebuild_archivos(candidato, preparado, origen_preparado, origen_bytes, manifiesto_bytes, contenidos, identificador=None):
    origen = preparado["source_candidate"]
    origen_hash = sha256(origen_bytes)
    if origen["manifest_sha256"] != origen_hash:
        raise ValueError("Hash del manifiesto del origen directo diferente del registrado.")
    esquema = preparado.get("schema_version")
    artefactos = _verificar_copia_rebuild(candidato, origen_preparado, contenidos, con_resultados=True, esquema=esquema)
    _verificar_resultados(candidato, contenidos)
    esperado = _manifiesto_rebuild(candidato, origen["id"], origen_hash, manifiesto_bytes, contenidos, artefactos,
                                  esquema, identificador)
    if preparado != esperado:
        raise ValueError("Manifiesto de reconstrucción incorrecto o hashes de artefactos diferentes.")
    return preparado


def reconstruir(store, candidato="initial", raiz_repositorio=RAIZ_REPOSITORIO):
    """Reconstruye desde initial o ingesta validada; un fallo conserva diagnóstico."""
    raiz = Path(raiz_repositorio).resolve()
    store = _destino(store, raiz)
    origen, preparado = _origen_rebuild(store, candidato, raiz)
    manifiesto_bytes, contenidos = _leer_referencias(raiz)
    origen_bytes = (origen / "manifest.json").read_bytes()
    if json.loads(origen_bytes) != preparado:
        raise ValueError("El manifiesto de origen cambió después de verificarlo.")
    esquema = 2 if preparado["stage"] == "historical-init" else preparado["schema_version"]
    contexto = capturar_contexto(store, raiz / MANIFIESTO_REFERENCIAS) if preparado["stage"] == "historical-init" else None
    nuevo = _ruta_candidato(store, f"rebuild-{uuid4().hex}")
    nuevo.mkdir(exist_ok=False)
    try:
        if contexto is not None:
            guardar_contexto(nuevo, contexto)
        for ruta, huella in preparado["artifacts"].items():
            contenido = (origen / ruta).read_bytes()
            if sha256(contenido) != huella:
                raise ValueError(f"El candidato de origen cambió durante la copia: {ruta}.")
            destino = nuevo / ruta
            destino.parent.mkdir(parents=True, exist_ok=True)
            with destino.open("xb") as archivo:
                archivo.write(contenido)
        _verificar_copia_rebuild(nuevo, preparado, contenidos, esquema=esquema)
        entradas = [nuevo / RECOMPUESTOS[ruta] for ruta in (SBS, BCRP)]
        for ruta in entradas:
            if not ruta.is_file():
                raise ValueError(f"Entrada requerida antes del constructor: {ruta}.")
        sbs_candidato = entradas[0].read_bytes()
        capturas = []

        def observar(datos):
            if capturas:
                raise ValueError("El constructor invocó el observador P75 más de una vez.")
            captura = auditar_p75(datos, sbs_candidato)
            verificar_cortes_referencia(captura, contenidos[SBS])
            capturas.append(captura)

        resultado = construir_dataset_analitico(
            ruta_sbs=entradas[0], ruta_bcrp=entradas[1],
            ruta_salida=nuevo / FEATURES_RECONSTRUIDAS, observador_p75=observar,
        )
        if len(capturas) != 1:
            raise ValueError("El constructor no entregó la observación P75 requerida.")
        auditoria = nuevo / AUDITORIA_P75
        auditoria.parent.mkdir(parents=True, exist_ok=True)
        with auditoria.open("x", encoding="utf-8") as archivo:
            json.dump(capturas[0], archivo, indent=2, ensure_ascii=False, allow_nan=False)
            archivo.write("\n")
        comparar_features(resultado, contenidos[FEATURES])
        verificar_etiquetas(resultado, capturas[0])
        _verificar_resultados(nuevo, contenidos)
        artefactos = _verificar_copia_rebuild(nuevo, preparado, contenidos, con_resultados=True, esquema=esquema)
        verificar(store, raiz, candidato=candidato)
        if (origen / "manifest.json").read_bytes() != origen_bytes or _leer_referencias(raiz)[0] != manifiesto_bytes:
            raise ValueError("Las referencias o el manifiesto de origen cambiaron durante la reconstrucción.")
        final = _manifiesto_rebuild(nuevo, candidato, sha256(origen_bytes), manifiesto_bytes, contenidos, artefactos, esquema)
        with (nuevo / "manifest.json").open("x", encoding="utf-8") as archivo:
            json.dump(final, archivo, indent=2, ensure_ascii=False, allow_nan=False)
            archivo.write("\n")
        return nuevo
    except Exception as error:
        marcador = nuevo / "manifest.json"
        if marcador.exists():
            marcador.unlink()
        diagnostico = nuevo / DIAGNOSTICO_REBUILD
        diagnostico.parent.mkdir(parents=True, exist_ok=True)
        with diagnostico.open("x", encoding="utf-8") as archivo:
            json.dump({
                "schema_version": 1, "stage": "historical-rebuild", "status": "failed",
                "candidate": nuevo.name, "path": str(nuevo),
                "source_candidate": {"id": candidato, "manifest_sha256": sha256(origen_bytes)},
                "error_type": type(error).__name__, "error": str(error),
            }, archivo, indent=2, ensure_ascii=False, allow_nan=False)
            archivo.write("\n")
        error.add_note(f"Reconstrucción no preparada. Diagnóstico y evidencia: {diagnostico}")
        raise


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


def _resultado_manifiesto(operacion, ruta, preparado=None, version=False):
    """Describe el manifiesto del destino usado, sin consultar LATEST."""
    contenido = (ruta / "manifest.json").read_bytes()
    manifiesto = json.loads(contenido)
    if preparado is not None and manifiesto != preparado:
        raise ValueError("El manifiesto cambió después de su verificación.")
    resultado = {
        "schema_version": 1, "operation": operacion,
        "version_id" if version else "candidate_id": ruta.name,
        "path": str(ruta), "manifest_sha256": sha256(contenido),
        "stage": manifiesto["stage"], "status": manifiesto["status"],
    }
    for campo in ("month", "source_candidate"):
        if campo in manifiesto:
            resultado[campo] = manifiesto[campo]
    return resultado


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    comandos = parser.add_subparsers(dest="comando", required=True)
    for nombre in ("init", "verify", "rebuild", "ingest", "publish", "resolve"):
        comando = comandos.add_parser(nombre)
        comando.add_argument("--store", required=True, type=Path)
        comando.add_argument("--json", action="store_true", help="Emite un único resultado JSON de éxito; progreso en stderr.")
        if nombre == "verify":
            selector = comando.add_mutually_exclusive_group()
            selector.add_argument("--candidate")
            selector.add_argument("--version")
        elif nombre == "publish":
            selector = comando.add_mutually_exclusive_group(required=True)
            selector.add_argument("--candidate")
            selector.add_argument("--resume-version")
        elif nombre in ("rebuild", "ingest"):
            comando.add_argument("--candidate", required=True)
        if nombre == "ingest":
            comando.add_argument("--month", required=True)
            comando.add_argument("--sbs-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        with redirect_stdout(sys.stderr) if args.json else nullcontext():
            if args.comando == "init":
                candidato = inicializar(args.store)
                if args.json:
                    resultado = _resultado_manifiesto("init", candidato)
                else:
                    print(f"Candidato preparado: {candidato}")
            elif args.comando == "rebuild":
                candidato = reconstruir(args.store, candidato=args.candidate)
                if args.json:
                    resultado = _resultado_manifiesto("rebuild", candidato)
                else:
                    print(f"Candidato reconstruido: {candidato.name}\nRuta: {candidato}")
            elif args.comando == "ingest":
                from src.pipeline.ingesta import ingerir
                candidato = ingerir(args.store, args.candidate, args.month, args.sbs_dir)
                if args.json:
                    resultado = _resultado_manifiesto("ingest", candidato)
                else:
                    print(f"Fuentes ingeridas y validadas: {candidato.name}\nRuta: {candidato}")
            elif args.comando == "publish":
                from src.pipeline.publicacion import publicar
                resultado = publicar(args.store, candidato=args.candidate, resume_version=args.resume_version)
                if args.json:
                    resultado = {"schema_version": 1, "operation": "publish",
                                 "status": "already-current" if resultado["already_current"] else "published", **resultado}
                else:
                    print(json.dumps(resultado, ensure_ascii=False))
            elif args.comando == "resolve":
                from src.pipeline.publicacion import resolver_latest
                resultado = resolver_latest(args.store).como_dict()
                if args.json:
                    resultado = {"schema_version": 1, "operation": "resolve", "status": "resolved", **resultado}
                else:
                    print(json.dumps(resultado, ensure_ascii=False))
            elif args.version is not None:
                from src.pipeline.versiones import ruta_version, verificar_version
                preparado = verificar_version(args.store, args.version)
                if args.json:
                    resultado = _resultado_manifiesto("verify", ruta_version(args.store.resolve(), args.version), preparado, version=True)
                else:
                    print(f"Versión verificada: {preparado['version_id']}")
            else:
                identificador = "initial" if args.candidate is None else args.candidate
                preparado = verificar(args.store, candidato=identificador)
                if args.json:
                    resultado = _resultado_manifiesto("verify", _ruta_candidato(args.store.resolve(), identificador), preparado)
                else:
                    print("Candidato verificado: 36 particiones, referencias y recomposición correctas.")
        if args.json:
            print(json.dumps(resultado, ensure_ascii=False, allow_nan=False))
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        for nota in getattr(error, "__notes__", ()):
            print(nota, file=sys.stderr)
        return getattr(error, "publication_exit_code", 1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
