"""Lock Linux/Python 3.14 desde las versiones del venv validado, sin instalar.

Uso: .venv/bin/python docker/airflow/generate_pipeline_lock.py
Los hashes proceden de los wheels publicados en PyPI para cada versión exacta.
No resuelve versiones nuevas ni incluye paquetes personales ajenos al pipeline.
"""

from importlib.metadata import distribution
import json
from pathlib import Path
import platform
import sys
import urllib.request

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


ROOT = Path(__file__).resolve().parents[2]


def validar_entorno():
    arquitectura = platform.machine().lower()
    implementacion = platform.python_implementation()
    if (sys.platform != "linux" or sys.version_info[:2] != (3, 14)
            or arquitectura not in {"amd64", "x86_64"} or implementacion != "CPython"):
        raise SystemExit(
            "El lock requiere Linux, CPython 3.14 y arquitectura amd64/x86_64; "
            f"recibido: {sys.platform}, Python {sys.version_info.major}.{sys.version_info.minor}, "
            f"implementación {implementacion}, arquitectura {arquitectura}. No se generó el lock."
        )


def main():
    validar_entorno()  # Antes de leer requisitos, consultar PyPI o escribir el lock.
    pendientes = [Requirement(linea) for linea in (ROOT / "requirements.txt").read_text().splitlines()
                  if linea.strip() and not linea.startswith("#")]
    pendientes.append(Requirement("ruff==0.17.1"))  # Mismo linter que el CI aprobado.
    versiones = {}
    while pendientes:
        requisito = pendientes.pop()
        if requisito.marker and not requisito.marker.evaluate({"extra": ""}):
            continue
        nombre = canonicalize_name(requisito.name)
        instalado = distribution(nombre)
        if instalado.version not in requisito.specifier:
            raise ValueError(f"Entorno incompatible: {requisito}, instalado {instalado.version}")
        if nombre in versiones:
            continue
        versiones[nombre] = instalado.version
        pendientes.extend(Requirement(r) for r in instalado.requires or [])

    lineas = ["# Generado desde el venv validado; Linux amd64, CPython 3.14.",
              "# Cierre completo de requirements.txt y Ruff del CI; solo wheels.",
              "# Los hashes permiten los wheels publicados de cada versión, no sdists."]
    for nombre, version in sorted(versiones.items()):
        url = f"https://pypi.org/pypi/{nombre}/{version}/json"
        with urllib.request.urlopen(url, timeout=60) as respuesta:
            datos = json.load(respuesta)
        hashes = sorted({a["digests"]["sha256"] for a in datos["urls"]
                         if a["packagetype"] == "bdist_wheel" and not a["yanked"]})
        if not hashes:
            raise ValueError(f"Sin wheels no retirados: {nombre}=={version}")
        lineas.append(f"{nombre}=={version} \\")
        lineas.extend(f"    --hash=sha256:{h}" + (" \\" if i < len(hashes) - 1 else "")
                      for i, h in enumerate(hashes))
    destino = ROOT / "docker/airflow/pipeline.lock"
    destino.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    print(f"{len(versiones)} dependencias fijadas en {destino.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
