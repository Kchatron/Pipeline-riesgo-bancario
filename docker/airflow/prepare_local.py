"""Prepara exclusivamente un estado local nuevo; secretos fuera del repositorio.

No inicia contenedores, no instala paquetes y nunca reutiliza un almacén.
"""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import subprocess
import tempfile
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[2]

def validar_imagen(image_id):
    if re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
        raise ValueError("--image-id requiere un ID completo sha256 de 64 caracteres hexadecimales; no una etiqueta.")
    try:
        imagenes = json.loads(subprocess.check_output(
            ["docker", "image", "inspect", image_id], stderr=subprocess.PIPE, text=True,
        ))
    except subprocess.CalledProcessError as error:
        raise ValueError(f"No se pudo inspeccionar la imagen local {image_id}; compruebe su existencia y el acceso a Docker.") from error
    if len(imagenes) != 1 or imagenes[0].get("Id") != image_id:
        raise ValueError("La identidad inspeccionada no coincide con --image-id.")
    if imagenes[0].get("Os") != "linux" or imagenes[0].get("Architecture") != "amd64":
        raise ValueError("La imagen local debe corresponder a Linux amd64.")


def escribir_nuevo(ruta, texto):
    with ruta.open("x", encoding="utf-8") as archivo:
        archivo.write(texto)
    ruta.chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent", type=Path, required=True)
    parser.add_argument("--image-id", required=True, help="ID completo sha256 de una imagen local previamente validada.")
    parser.add_argument("--sbs-dir", type=Path, default=ROOT / "data/raw/SBS")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    validar_imagen(args.image_id)
    parent, sbs = args.parent.resolve(strict=True), args.sbs_dir.resolve(strict=True)
    if os.getuid() == 0 or not sbs.is_dir() or not 1024 <= args.port <= 65535:
        raise ValueError("Se requieren UID no root, directorio SBS y puerto local entre 1024 y 65535.")
    filesystem = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "-T", str(parent)], text=True).strip()
    if filesystem != "ext4":
        raise ValueError(f"El estado nuevo requiere ext4; recibido {filesystem}.")
    if parent.is_relative_to(ROOT):
        raise ValueError("El estado debe estar fuera del repositorio.")
    with socket.socket() as puerto:
        puerto.bind(("127.0.0.1", args.port))
    memoria = {fila.split(":", 1)[0]: int(fila.split()[1]) * 1024
               for fila in Path("/proc/meminfo").read_text().splitlines() if ":" in fila and len(fila.split()) >= 2}
    referencia = json.loads((ROOT / "config/referencias_historico.json").read_text())
    for r in referencia["references"]:
        if hashlib.sha256((ROOT / r["path"]).read_bytes()).hexdigest() != r["sha256"]:
            raise ValueError(f"Referencia alterada: {r['path']}")
    os.umask(0o077)
    estado = Path(tempfile.mkdtemp(prefix="airflow-local-", dir=parent))
    for nombre in ("logs", "auth", "data", "secrets", "evidence"):
        (estado / nombre).mkdir(mode=0o700)
    proyecto = "historico-airflow-" + uuid4().hex[:10]
    password_pg = secrets.token_urlsafe(32)
    escribir_nuevo(estado / "secrets/postgres.env", f"POSTGRES_PASSWORD={password_pg}\n")
    escribir_nuevo(estado / "secrets/airflow.env", "\n".join([
        f"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:{password_pg}@postgres/airflow",
        "AIRFLOW__CORE__FERNET_KEY=" + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "AIRFLOW__API_AUTH__JWT_SECRET=" + secrets.token_urlsafe(64),
        "AIRFLOW__API__SECRET_KEY=" + secrets.token_urlsafe(64),
    ]) + "\n")
    escribir_nuevo(estado / "auth/passwords.json", json.dumps({"admin": secrets.token_urlsafe(32)}) + "\n")
    variables = {
        "COMPOSE_PROJECT_NAME": proyecto, "AIRFLOW_IMAGE": args.image_id, "AIRFLOW_UID": str(os.getuid()),
        "AIRFLOW_PORT": str(args.port), "AIRFLOW_ADMIN_USER": "admin",
        "AIRFLOW_LOGS_DIR": str(estado / "logs"), "AIRFLOW_AUTH_DIR": str(estado / "auth"),
        "PIPELINE_MOUNT_DIR": str(estado / "data"), "SBS_SOURCE_DIR": str(sbs),
        "AIRFLOW_SECRETS_FILE": str(estado / "secrets/airflow.env"),
        "POSTGRES_SECRETS_FILE": str(estado / "secrets/postgres.env"),
    }
    escribir_nuevo(estado / "local.env", "\n".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in variables.items()) + "\n")
    previo = {
        "containers": subprocess.check_output(["docker", "ps", "-aq"], text=True).split(),
        "volumes": subprocess.check_output(["docker", "volume", "ls", "-q"], text=True).split(),
        "image_id": args.image_id, "references": referencia,
        "filesystem": filesystem, "disk_free_bytes": shutil.disk_usage(parent).free,
        "memory_available_bytes": memoria["MemAvailable"],
    }
    escribir_nuevo(estado / "evidence/before.json", json.dumps(previo, indent=2))
    print(json.dumps({"state": str(estado), "env_file": str(estado / "local.env"),
                      "project": proyecto, "uid": os.getuid(), "gid": 0,
                      "filesystem": filesystem, "port": args.port,
                      "pipeline_store": str(estado / "data/historico")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
