"""Operador Airflow: ejecuta una CLI y devuelve solo una identidad por XCom."""

import re

from airflow.sdk import BaseOperator

from src.orquestacion.adaptador_cli import (
    AdaptadorCLI,
    CancelacionCLI,
    CANDIDATO,
    ErrorContratoCLI,
    HASH,
    VERSION,
)


PATRON_PERIODO = r"202[3-5]-(?:0[1-9]|1[0-2])"


def _identidad(resultado):
    """No propaga rutas, logs ni otros campos del resultado CLI hacia XCom."""
    campo = "version_id" if "version_id" in resultado else "candidate_id"
    identificador, huella = resultado.get(campo), resultado.get("manifest_sha256")
    patron = VERSION if campo == "version_id" else CANDIDATO
    if (not isinstance(identificador, str) or re.fullmatch(patron, identificador) is None
            or not isinstance(huella, str) or re.fullmatch(HASH, huella) is None):
        raise ErrorContratoCLI("Identificador o hash de transferencia inválido.")
    return {campo: identificador, "manifest_sha256": huella}


class HistoricoOperator(BaseOperator):
    """Una operación de la cadena mensual; no crea procesos durante el parsing."""

    def __init__(self, *, operacion, origen_task_id=None, store="/store/historico",
                 sbs_dir="/sources/sbs", **kwargs):
        super().__init__(**kwargs)
        if operacion not in ("ingest", "verify", "rebuild", "publish"):
            raise ValueError("Operación mensual no admitida.")
        if (operacion == "ingest") != (origen_task_id is None):
            raise ValueError("Solo ingest carece de tarea de origen.")
        self.operacion = operacion
        self.origen_task_id = origen_task_id
        self.store = store
        self.sbs_dir = sbs_dir
        self._adaptador = None
        self._cancelado = False

    def execute(self, context):
        periodo = context.get("params", {}).get("periodo")
        if not isinstance(periodo, str) or re.fullmatch(PATRON_PERIODO, periodo) is None:
            raise ValueError("periodo es obligatorio: YYYY-MM entre 2023-01 y 2025-12.")
        opciones = {"store": self.store}
        origen = None
        if self.operacion == "ingest":
            opciones.update(candidato="initial", mes=periodo, sbs_dir=self.sbs_dir)
        else:
            dato = context["ti"].xcom_pull(task_ids=self.origen_task_id, key="return_value")
            if not isinstance(dato, dict):
                raise ErrorContratoCLI("Falta una identidad XCom del origen.")
            origen = _identidad(dato)
            if dato != origen:
                raise ErrorContratoCLI("XCom debe contener exclusivamente identificador y hash.")
            if "version_id" in origen:
                if self.operacion != "verify":
                    raise ErrorContratoCLI("Solo verify admite una versión como origen.")
                opciones["version"] = origen["version_id"]
            else:
                opciones["candidato"] = origen["candidate_id"]
        if self._cancelado:
            raise CancelacionCLI("Tarea cancelada antes de iniciar el adaptador.")
        # Se crea durante execute: nunca se serializa un proceso o candado en el DAG.
        self._adaptador = AdaptadorCLI()
        try:
            # on_kill puede interrumpir el constructor antes de asignar el adaptador.
            if self._cancelado:
                self._adaptador.cancelar()
            resultado = self._adaptador.ejecutar(self.operacion, **opciones)
            identidad = _identidad(resultado)
            if self.operacion == "verify" and identidad != origen:
                raise ErrorContratoCLI("La verificación devolvió otro identificador o hash de manifiesto.")
            if self.operacion == "rebuild" and resultado["source_candidate"] != {
                "id": origen["candidate_id"], "manifest_sha256": origen["manifest_sha256"],
            }:
                raise ErrorContratoCLI("La reconstrucción no usó el manifiesto ingerido verificado.")
            if self.operacion == "publish" and identidad["version_id"] != "v-" + origen["manifest_sha256"]:
                raise ErrorContratoCLI("La publicación no corresponde al manifiesto reconstruido verificado.")
            if self._cancelado:
                raise CancelacionCLI("Tarea cancelada; inspeccione evidencia, sin asumir LATEST intacto.")
            return identidad
        finally:
            self._adaptador = None

    def on_kill(self):
        self._cancelado = True
        if self._adaptador is not None:
            self._adaptador.cancelar()
