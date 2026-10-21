"""Subproceso aislado y contrato JSON, sin importar Airflow ni ejecutar al importar."""

import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from threading import RLock


LANZADOR = "/opt/pipeline-python"
DIRECTORIO = "/opt/project"
HASH = r"[0-9a-f]{64}"
CANDIDATO = r"(?:initial|ingest-[0-9a-f]{32}|rebuild-[0-9a-f]{32})"
VERSION = rf"v-{HASH}"


class ErrorCLI(RuntimeError):
    """Conserva el código original y ambas salidas, incluida evidencia de código 4."""

    def __init__(self, codigo, stdout, stderr):
        self.returncode, self.stdout, self.stderr = codigo, stdout, stderr
        super().__init__(f"CLI histórica terminó con código {codigo}: {stderr}")


class ErrorContratoCLI(ValueError):
    """Código cero con una respuesta que no cumple el contrato esperado."""


class CancelacionCLI(RuntimeError):
    """Ejecución cancelada; no implica que LATEST permanezca intacto."""


def _exigir(condicion, mensaje):
    if not condicion:
        raise ErrorContratoCLI(mensaje)


def _coincide(patron, valor):
    return isinstance(valor, str) and re.fullmatch(patron, valor) is not None


def _ruta(valor):
    ruta = Path(valor)
    if not ruta.is_absolute() or ".." in ruta.parts:
        raise ValueError("Se requiere una ruta absoluta sin componentes '..'.")
    return str(ruta)


def _argumentos(operacion, store, candidato, version, mes, sbs_dir):
    if operacion not in ("ingest", "verify", "rebuild", "publish"):
        raise ValueError("Operación no admitida por el adaptador mensual.")
    if candidato is not None and not _coincide(CANDIDATO, candidato):
        raise ValueError("Identificador de candidato inválido.")
    if version is not None and not _coincide(VERSION, version):
        raise ValueError("Identificador de versión inválido.")
    if version is not None and (operacion != "verify" or candidato is not None):
        raise ValueError("--version solo se admite en verify y excluye --candidate.")
    if operacion == "verify" and candidato is None and version is None:
        candidato = "initial"
    if operacion != "verify" and candidato is None:
        raise ValueError("La operación requiere un candidato explícito.")
    if operacion == "ingest" and candidato != "initial":
        raise ValueError("La ingesta mensual requiere initial.")
    if operacion == "rebuild" and candidato.startswith("rebuild-"):
        raise ValueError("No se admiten cadenas de reconstrucción.")
    if operacion == "publish" and not candidato.startswith("rebuild-"):
        raise ValueError("Publicar requiere un candidato reconstruido.")
    if operacion == "ingest":
        if not _coincide(r"202[3-5]-(?:0[1-9]|1[0-2])", mes) or sbs_dir is None:
            raise ValueError("Ingest requiere mes 2023-01–2025-12 y directorio SBS.")
    elif mes is not None or sbs_dir is not None:
        raise ValueError("Mes y directorio SBS solo se admiten en ingest.")
    argumentos = [LANZADOR, "-m", "src.pipeline.historico", operacion, "--store", _ruta(store), "--json"]
    if candidato is not None:
        argumentos.extend(["--candidate", candidato])
    if version is not None:
        argumentos.extend(["--version", version])
    if operacion == "ingest":
        argumentos.extend(["--month", mes, "--sbs-dir", _ruta(sbs_dir)])
    return argumentos, candidato


def _objeto(pares):
    resultado = {}
    for clave, valor in pares:
        _exigir(clave not in resultado, f"Campo JSON duplicado: {clave}.")
        resultado[clave] = valor
    return resultado


def _validar(stdout, operacion, store, candidato, version, mes):
    def constante_invalida(valor):
        raise ErrorContratoCLI(f"Constante JSON no permitida: {valor}.")

    try:
        resultado = json.loads(stdout, object_pairs_hook=_objeto, parse_constant=constante_invalida)
    except (ValueError, TypeError) as error:
        raise ErrorContratoCLI("stdout debe contener exactamente un documento JSON válido.") from error
    _exigir(isinstance(resultado, dict), "El resultado debe ser un objeto JSON.")
    _exigir(type(resultado.get("schema_version")) is int and resultado["schema_version"] == 1,
            "schema_version no admitido.")
    _exigir(resultado.get("operation") == operacion, "Operación de respuesta diferente de la solicitada.")
    _exigir(_coincide(HASH, resultado.get("manifest_sha256")), "Hash de manifiesto inválido.")
    campos = {"schema_version", "operation", "status", "path", "manifest_sha256"}
    es_version = version is not None or operacion == "publish"
    identificador = resultado.get("version_id" if es_version else "candidate_id")
    _exigir(_coincide(VERSION if es_version else CANDIDATO, identificador), "Identificador de respuesta inválido.")
    campos.add("version_id" if es_version else "candidate_id")
    esperado = version if es_version else candidato
    if operacion == "verify":
        _exigir(identificador == esperado, "Verify respondió por otro candidato o versión.")
    elif operacion != "publish":
        _exigir(identificador.startswith(operacion + "-"), "Etapa de candidato incorrecta.")
    ruta = Path(store) / ("versions" if es_version else "work") / identificador
    _exigir(resultado.get("path") == str(ruta), "Ruta no correspondiente al identificador y almacén solicitados.")
    if operacion == "publish":
        campos.add("already_current")
        actual = resultado.get("already_current")
        _exigir(type(actual) is bool, "already_current debe ser booleano.")
        _exigir(resultado.get("status") == ("already-current" if actual else "published"), "Estado de publicación incoherente.")
    else:
        campos.add("stage")
        etapa = "version" if es_version else ("init" if identificador == "initial" else identificador.split("-", 1)[0])
        estado = {"version": "complete", "init": "prepared", "ingest": "sources-validated", "rebuild": "prepared"}[etapa]
        _exigir(resultado.get("stage") == "historical-" + etapa and resultado.get("status") == estado,
                "Etapa o estado de respuesta inválidos.")
        if etapa in ("ingest", "rebuild"):
            campos.add("source_candidate")
            origen = resultado.get("source_candidate")
            _exigir(isinstance(origen, dict) and set(origen) == {"id", "manifest_sha256"}, "Origen de candidato inválido.")
            _exigir(_coincide(r"(?:initial|ingest-[0-9a-f]{32})", origen["id"])
                    and _coincide(HASH, origen["manifest_sha256"]), "Identidad de origen inválida.")
            if operacion != "verify":
                _exigir(origen["id"] == candidato, "El resultado procede de otro candidato.")
            if etapa == "ingest":
                campos.add("month")
                _exigir(origen["id"] == "initial", "Origen de ingesta incorrecto.")
                _exigir(_coincide(r"202[3-5]-(?:0[1-9]|1[0-2])", resultado.get("month")), "Mes de respuesta inválido.")
                if operacion == "ingest":
                    _exigir(resultado["month"] == mes, "El resultado corresponde a otro mes.")
    _exigir(set(resultado) == campos, "Campos de respuesta diferentes del contrato JSON esquema 1.")
    return resultado


class AdaptadorCLI:
    """Una instancia por operación. cancelar() será el punto de conexión del futuro on_kill."""

    def __init__(self):
        self._lock = RLock()
        self._proceso = None
        self._iniciado = False
        self._cancelado = False

    def cancelar(self):
        """Mata el grupo propio, sin reparar ni borrar evidencia; ejecutar() espera al hijo."""
        with self._lock:
            self._cancelado = True
            if self._proceso is not None:
                try:
                    os.killpg(self._proceso.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass  # El grupo ya terminó; no es una reparación del almacén.

    def ejecutar(self, operacion, *, store, candidato=None, version=None, mes=None, sbs_dir=None):
        argumentos, candidato = _argumentos(operacion, store, candidato, version, mes, sbs_dir)
        with self._lock:
            if self._cancelado:
                raise CancelacionCLI("Cancelado antes de iniciar la CLI.")
            if self._iniciado:
                raise RuntimeError("Utilice una instancia de AdaptadorCLI por operación.")
            self._iniciado = True
            self._proceso = subprocess.Popen(argumentos, cwd=DIRECTORIO, start_new_session=True,
                                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            try:
                with self._lock:
                    # on_kill puede marcar cancelación dentro de Popen, antes de
                    # que su resultado haya quedado asignado a self._proceso.
                    if self._cancelado:
                        self.cancelar()
                stdout, stderr = self._proceso.communicate()
            except BaseException as error:
                try:
                    self.cancelar()
                    _, stderr = self._proceso.communicate()
                    sys.stderr.write(stderr.decode("utf-8", errors="replace"))
                except Exception as limpieza:
                    error.add_note(f"No se pudo confirmar terminación de la CLI: {limpieza}")
                raise
            stderr = stderr.decode("utf-8", errors="replace")
            sys.stderr.write(stderr)
            with self._lock:
                if self._cancelado:
                    raise CancelacionCLI("CLI cancelada. Inspeccione evidencia; LATEST puede haber cambiado.")
                if self._proceso.returncode != 0:
                    raise ErrorCLI(self._proceso.returncode, stdout.decode("utf-8", errors="replace"), stderr)
                try:
                    texto = stdout.decode("utf-8", errors="strict")
                except UnicodeError as error:
                    raise ErrorContratoCLI("stdout debe ser UTF-8 válido.") from error
                resultado = _validar(texto, operacion, _ruta(store), candidato, version, mes)
                # El mismo hilo puede entrar en cancelar() mientras _validar corre.
                if self._cancelado:
                    raise CancelacionCLI("CLI cancelada durante la validación; no se devuelve éxito.")
                return resultado
        finally:
            with self._lock:
                self._proceso = None
