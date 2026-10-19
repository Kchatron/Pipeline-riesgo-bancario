"""CLI limitada a inicializar y verificar un candidato histórico, sin publicarlo."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import sys

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


def _verificar_contenido(candidato, contenidos):
    if candidato.is_symlink() or candidato.parent.is_symlink():
        raise ValueError("El candidato no puede ser un enlace simbólico.")
    archivos = set()
    for ruta in candidato.rglob("*"):
        if ruta.is_symlink():
            raise ValueError(f"Enlace simbólico no permitido en el candidato: {ruta.name}.")
        if ruta.is_file():
            archivos.add(ruta.relative_to(candidato).as_posix())
    if archivos - {"manifest.json"} != _artefactos_esperados():
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
        for ruta in sorted(_artefactos_esperados())
    }


def verificar(store, raiz_repositorio=RAIZ_REPOSITORIO):
    """Comprueba un candidato preparado sin escribir ni reparar archivos."""
    raiz = Path(raiz_repositorio).resolve()
    store = _destino(store, raiz)
    manifiesto_bytes, contenidos = _leer_referencias(raiz)
    candidato = store / CANDIDATO
    marcador = candidato / "manifest.json"
    if not marcador.is_file() or marcador.is_symlink():
        raise ValueError("Candidato incompleto: falta un manifiesto preparado válido.")
    preparado = json.loads(marcador.read_bytes())
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
    for nombre in ("init", "verify"):
        comando = comandos.add_parser(nombre)
        comando.add_argument("--store", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.comando == "init":
            candidato = inicializar(args.store)
            print(f"Candidato preparado: {candidato}")
        else:
            verificar(args.store)
            print("Candidato verificado: 36 particiones, referencias y recomposición correctas.")
    except (OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
