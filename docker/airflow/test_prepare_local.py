"""Preparación local con Docker simulado y escrituras solo en temporales."""

from importlib import util
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import MagicMock

import pytest


IMAGE_ID = "sha256:" + "a" * 64


@pytest.fixture
def preparador():
    spec = util.spec_from_file_location("prepare_local", Path(__file__).with_name("prepare_local.py"))
    modulo = util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def test_image_id_obligatorio(preparador, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(sys, "argv", ["prepare_local.py", "--parent", str(tmp_path)])
    inspect = MagicMock(side_effect=AssertionError("No consultar Docker sin argumento"))
    monkeypatch.setattr(preparador.subprocess, "check_output", inspect)
    with pytest.raises(SystemExit) as error:
        preparador.main()
    assert error.value.code == 2
    assert "--image-id" in capsys.readouterr().err
    inspect.assert_not_called()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("image_id", ["imagen:latest", "sha256:abc", "a" * 64])
def test_rechaza_etiquetas_e_ids_abreviados(preparador, monkeypatch, image_id):
    inspect = MagicMock()
    monkeypatch.setattr(preparador.subprocess, "check_output", inspect)
    with pytest.raises(ValueError, match="ID completo"):
        preparador.validar_imagen(image_id)
    inspect.assert_not_called()


def test_imagen_inexistente(preparador, monkeypatch):
    original = subprocess.CalledProcessError(1, ["docker", "image", "inspect", IMAGE_ID], stderr="No such image")
    monkeypatch.setattr(preparador.subprocess, "check_output", MagicMock(side_effect=original))
    with pytest.raises(ValueError, match="existencia y el acceso a Docker") as error:
        preparador.validar_imagen(IMAGE_ID)
    assert error.value.__cause__ is original


@pytest.mark.parametrize("sistema,arquitectura", [("windows", "amd64"), ("linux", "arm64")])
def test_plataforma_incorrecta(preparador, monkeypatch, sistema, arquitectura):
    respuesta = [{"Id": IMAGE_ID, "Os": sistema, "Architecture": arquitectura}]
    monkeypatch.setattr(preparador.subprocess, "check_output", MagicMock(return_value=json.dumps(respuesta)))
    with pytest.raises(ValueError, match="Linux amd64"):
        preparador.validar_imagen(IMAGE_ID)


def test_identidad_distinta(preparador, monkeypatch):
    respuesta = [{"Id": "sha256:" + "b" * 64, "Os": "linux", "Architecture": "amd64"}]
    monkeypatch.setattr(preparador.subprocess, "check_output", MagicMock(return_value=json.dumps(respuesta)))
    with pytest.raises(ValueError, match="identidad"):
        preparador.validar_imagen(IMAGE_ID)


def test_preparacion_conserva_id_exacto(preparador, monkeypatch, tmp_path, capsys):
    root = tmp_path / "repo"
    (root / "config").mkdir(parents=True)
    (root / "config/referencias_historico.json").write_text('{"references": []}')
    sbs = tmp_path / "sbs"
    sbs.mkdir()
    parent = tmp_path / "externo"
    parent.mkdir()
    monkeypatch.setattr(preparador, "ROOT", root)
    monkeypatch.setattr(preparador.os, "getuid", lambda: 1000)
    monkeypatch.setattr(preparador.os, "umask", MagicMock())
    monkeypatch.setattr(preparador.socket, "socket", MagicMock())

    def consultar(comando, **kwargs):
        if comando == ["docker", "image", "inspect", IMAGE_ID]:
            assert kwargs == {"stderr": subprocess.PIPE, "text": True}
            return json.dumps([{"Id": IMAGE_ID, "Os": "linux", "Architecture": "amd64"}])
        if comando == ["findmnt", "-n", "-o", "FSTYPE", "-T", str(parent)]:
            return "ext4\n"
        if comando in (["docker", "ps", "-aq"], ["docker", "volume", "ls", "-q"]):
            return ""
        raise AssertionError(f"Comando inesperado: {comando}")

    monkeypatch.setattr(preparador.subprocess, "check_output", consultar)
    monkeypatch.setattr(sys, "argv", ["prepare_local.py", "--parent", str(parent),
                                     "--sbs-dir", str(sbs), "--image-id", IMAGE_ID])
    preparador.main()
    resultado = json.loads(capsys.readouterr().out)
    env = dict(line.split("=", 1) for line in Path(resultado["env_file"]).read_text().splitlines())
    assert json.loads(env["AIRFLOW_IMAGE"]) == IMAGE_ID
    evidencia = json.loads((Path(resultado["state"]) / "evidence/before.json").read_text())
    assert evidencia["image_id"] == IMAGE_ID
    assert list((Path(resultado["state"]) / "data").iterdir()) == []
