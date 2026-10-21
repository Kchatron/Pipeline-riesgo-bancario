"""Validación local sin red, con el mismo código/venv incorporado en la imagen."""

import argparse
import hashlib
from importlib import metadata, util
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path("/opt/project")
TESTS = {
    "test_calidad_datos.py", "test_features.py", "test_bcrp_extractor.py",
    "test_historico_init.py", "test_historico_rebuild.py", "test_historico_rebuild_cli.py",
    "test_historico_cli_json.py", "test_historico_publish.py", "test_publication_package.py",
    "test_historico_ingest.py", "test_historico_ingest_rebuild.py",
}


def huella(ruta):
    return hashlib.sha256(ruta.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--require-ext4", action="store_true")
    parser.add_argument("--require-uid", type=int)
    args = parser.parse_args()
    if sys.prefix != "/opt/pipeline-venv" or sys.prefix == sys.base_prefix:
        raise ValueError("Se requiere el entorno aislado /opt/pipeline-venv")
    if util.find_spec("airflow") is not None:
        raise ValueError("Airflow no debe ser importable desde el entorno del pipeline")
    if "include-system-site-packages = false" not in Path(sys.prefix, "pyvenv.cfg").read_text():
        raise ValueError("El venv no puede heredar site-packages")
    if args.require_uid is not None and (os.getuid() != args.require_uid or os.getuid() == 0 or os.getgid() != 0):
        raise ValueError("Se requiere el UID no root del host y GID 0")
    mascara = os.umask(0)
    os.umask(mascara)
    if mascara != 0o022:
        raise ValueError("Ejecutar mediante /opt/pipeline-python: se requiere umask 0022")

    # Importación explícita del código incorporado, sin montar otra copia.
    sys.path.insert(0, str(ROOT))
    import pandas as pd
    from src.pipeline import historico as h, publicacion as p
    from src.pipeline.validacion_rebuild import leer_features_exactas

    store = args.store.resolve()
    if store.exists() or store.is_relative_to(ROOT):
        raise ValueError("El almacén debe ser nuevo y externo a /opt/project")
    filesystem = p._tipo_filesystem(store.parent)
    if args.require_ext4 and filesystem != "ext4":
        raise ValueError(f"Se requiere ext4, recibido {filesystem}")
    evidencia = store.with_name(store.name + "-validation")
    evidencia.mkdir()  # Repetir exige otro destino; no borra evidencia.
    referencias = json.loads((ROOT / h.MANIFIESTO_REFERENCIAS).read_bytes())
    datos_presentes = {ruta.relative_to(ROOT).as_posix() for ruta in (ROOT / "data").rglob("*") if ruta.is_file()}
    if datos_presentes != {r["path"] for r in referencias["references"]}:
        raise ValueError("La imagen debe contener exclusivamente los cinco CSV aprobados, sin Excel")
    if {ruta.name for ruta in (ROOT / "tests").iterdir()} != TESTS:
        raise ValueError("Inventario inesperado de pruebas en la imagen")

    def comprobar_referencias():
        for referencia in referencias["references"]:
            if huella(ROOT / referencia["path"]) != referencia["sha256"]:
                raise ValueError(f"Referencia alterada: {referencia['path']}")

    comprobar_referencias()
    comandos = []

    def cli(*argumentos):
        comando = [sys.executable, "-B", "-m", "src.pipeline.historico", argumentos[0],
                   "--store", str(store), *argumentos[1:], "--json"]
        resultado = subprocess.run(comando, cwd=ROOT, text=True, capture_output=True)
        indice = len(comandos)
        (evidencia / f"{indice}.stdout").write_text(resultado.stdout)
        (evidencia / f"{indice}.stderr").write_text(resultado.stderr)
        comandos.append({"argv": comando, "returncode": resultado.returncode})
        (evidencia / "commands.json").write_text(json.dumps(comandos, indent=2))
        if resultado.returncode:
            raise RuntimeError(f"Falló {argumentos[0]}; evidencia: {evidencia}")
        return json.loads(resultado.stdout)

    cli("init")
    cli("verify")
    rebuilt = cli("rebuild", "--candidate", "initial")
    cli("verify", "--candidate", rebuilt["candidate_id"])
    candidato = Path(rebuilt["path"])
    actual = leer_features_exactas((candidato / h.FEATURES_RECONSTRUIDAS).read_bytes())
    esperado = leer_features_exactas((ROOT / "data/processed/dataset_ml_features.csv").read_bytes())
    claves = ["fecha_id", "banco_id", "tipo_credito_id"]
    pd.testing.assert_frame_equal(actual.set_index(claves).sort_index(),
                                  esperado.set_index(claves).sort_index(), check_exact=True)
    assert actual.shape == (340, 23)
    auditoria = json.loads((candidato / "audit/p75.json").read_bytes())
    assert auditoria["equal"] and len(auditoria["panels"]) == 10
    for panel in auditoria["panels"]:
        assert panel["count"] == 24 and panel["equal"]
        assert panel["observed_cut"] == panel["expected_cut"]
    comprobar_referencias()
    inventario = {ruta.relative_to(ROOT).as_posix(): huella(ruta)
                  for carpeta in ("src", "config", "data", "tests", "docker")
                  for ruta in (ROOT / carpeta).rglob("*") if ruta.is_file()}
    for nombre in inventario:
        if (ROOT / nombre).stat().st_mode & 0o222:
            raise ValueError(f"Código, configuración, pruebas o referencias escribibles: {nombre}")
    informe = {"python": sys.version, "executable": sys.executable,
               "uid": os.getuid(), "gid": os.getgid(), "umask": oct(mascara), "filesystem": filesystem,
               "candidate": rebuilt, "features_shape": list(actual.shape), "p75": auditoria,
               "installed": {d.metadata["Name"]: d.version for d in metadata.distributions()},
               "image_files_sha256": inventario, "references_verified": True}
    (evidencia / "result.json").write_text(json.dumps(informe, ensure_ascii=False, indent=2, allow_nan=False))
    print(json.dumps({"status": "passed", "candidate_id": rebuilt["candidate_id"],
                      "shape": list(actual.shape), "p75_panels": 10, "evidence": str(evidencia)}))


if __name__ == "__main__":
    main()
