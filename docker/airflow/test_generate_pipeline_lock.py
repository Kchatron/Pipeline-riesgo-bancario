"""Guardas del generador: sin PyPI, instalaciones ni escritura del lock."""

from importlib import util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


@pytest.fixture
def generador():
    spec = util.spec_from_file_location("generate_pipeline_lock", Path(__file__).with_name("generate_pipeline_lock.py"))
    modulo = util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


@pytest.mark.parametrize("sistema,version,arquitectura,implementacion", [
    ("win32", (3, 14), "AMD64", "CPython"), ("darwin", (3, 14), "x86_64", "CPython"),
    ("linux", (3, 13), "x86_64", "CPython"), ("linux", (3, 15), "x86_64", "CPython"),
    ("linux", (3, 14), "aarch64", "CPython"), ("linux", (3, 14), "i686", "CPython"),
    ("linux", (3, 14), "x86_64", "PyPy"),
])
def test_rechazo_antes_de_lectura_red_o_escritura(generador, sistema, version, arquitectura, implementacion):
    class Version(tuple):
        major = version[0]
        minor = version[1]

    sistema_simulado = SimpleNamespace(platform=sistema, version_info=Version(version))
    with patch.object(generador, "sys", sistema_simulado), \
            patch.object(generador.platform, "machine", return_value=arquitectura), \
            patch.object(generador.platform, "python_implementation", return_value=implementacion), \
            patch.object(generador, "distribution", side_effect=AssertionError("No consultar paquetes")), \
            patch.object(generador.urllib.request, "urlopen", side_effect=AssertionError("No consultar PyPI")), \
            patch.object(Path, "read_text", side_effect=AssertionError("No leer requisitos")), \
            patch.object(Path, "write_text", side_effect=AssertionError("No escribir lock")):
        with pytest.raises(SystemExit) as error:
            generador.main()
    assert isinstance(error.value.code, str)
    assert "Linux, CPython 3.14 y arquitectura amd64/x86_64" in error.value.code
    assert sistema in error.value.code and arquitectura.lower() in error.value.code
    assert implementacion in error.value.code
    assert f"Python {version[0]}.{version[1]}" in error.value.code


@pytest.mark.parametrize("arquitectura", ["amd64", "AMD64", "x86_64"])
def test_entorno_admitido(generador, arquitectura):
    sistema_simulado = SimpleNamespace(platform="linux", version_info=(3, 14, 8))
    with patch.object(generador, "sys", sistema_simulado), \
            patch.object(generador.platform, "machine", return_value=arquitectura), \
            patch.object(generador.platform, "python_implementation", return_value="CPython"):
        generador.validar_entorno()
