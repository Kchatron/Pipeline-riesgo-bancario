"""Pruebas con Airflow real y CLI simulada; sin DB, red ni fuentes privadas."""

from copy import deepcopy
import json
import os
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

import airflow
from airflow.dag_processing.dagbag import DagBag
from airflow.exceptions import ParamValidationError
from airflow.serialization.serialized_objects import DagSerialization

from src.orquestacion import adaptador_cli as a, operador_historico as o


RAIZ = Path(__file__).resolve().parents[1]
DAG_FOLDER = Path(os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", RAIZ / "dags"))
INGEST = "ingest-" + "a" * 32
REBUILD = "rebuild-" + "b" * 32
HASH_INGEST = "1" * 64
HASH_REBUILD = "2" * 64
HASH_VERSION = "3" * 64
VERSION = "v-" + HASH_REBUILD
CADENA = ("ingest", "verify_ingest", "rebuild", "verify_rebuild", "publish", "verify_version")


def respuestas():
    ingest = {"schema_version": 1, "operation": "ingest", "candidate_id": INGEST,
              "manifest_sha256": HASH_INGEST, "path": f"/store/historico/work/{INGEST}",
              "stage": "historical-ingest", "status": "sources-validated", "month": "2024-06",
              "source_candidate": {"id": "initial", "manifest_sha256": "0" * 64}}
    rebuild = {"schema_version": 1, "operation": "rebuild", "candidate_id": REBUILD,
               "manifest_sha256": HASH_REBUILD, "path": f"/store/historico/work/{REBUILD}",
               "stage": "historical-rebuild", "status": "prepared",
               "source_candidate": {"id": INGEST, "manifest_sha256": HASH_INGEST}}
    publish = {"schema_version": 1, "operation": "publish", "version_id": VERSION,
               "manifest_sha256": HASH_VERSION, "path": f"/store/historico/versions/{VERSION}",
               "status": "published", "already_current": False}
    return {
        "ingest": ingest, "verify_ingest": {**ingest, "operation": "verify"},
        "rebuild": rebuild, "verify_rebuild": {**rebuild, "operation": "verify"}, "publish": publish,
        "verify_version": {"schema_version": 1, "operation": "verify", "version_id": VERSION,
                           "manifest_sha256": HASH_VERSION, "path": publish["path"],
                           "stage": "historical-version", "status": "complete"},
    }


class TestHistoricoMensual(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with patch.object(o, "AdaptadorCLI") as crear, patch.object(a.subprocess, "Popen") as iniciar:
            cls.bag = DagBag(dag_folder=DAG_FOLDER, safe_mode=False, known_pools={"default_pool"})
            crear.assert_not_called()
            iniciar.assert_not_called()
        cls.dag = cls.bag.dags["historico_mensual"]

    def setUp(self):
        self.popen = self.enterContext(patch.object(a.subprocess, "Popen"))
        self.enterContext(patch.object(a.os, "killpg", side_effect=AssertionError("No señales reales")))

    def contexto(self, dato=None, periodo="2024-06"):
        ti = MagicMock()
        ti.xcom_pull.return_value = dato
        ti.xcom_push.side_effect = AssertionError("XCom se emite solo mediante return_value")
        return {"params": {"periodo": periodo}, "ti": ti}

    def tarea(self, task_id):
        index = CADENA.index(task_id)
        return o.HistoricoOperator(task_id=task_id, operacion=("ingest", "verify", "rebuild", "verify", "publish", "verify")[index],
                                   origen_task_id=None if index == 0 else CADENA[index - 1], retries=0)

    def responder(self, dato, codigo=0, stderr=b""):
        hijo = MagicMock(pid=12345, returncode=codigo)
        texto = dato if isinstance(dato, bytes) else json.dumps(dato).encode()
        hijo.communicate.return_value = (texto, stderr)
        self.popen.return_value = hijo

    def test_importacion_en_version_fijada_sin_efectos(self):
        self.assertEqual(airflow.__version__, "3.3.2")
        self.assertEqual(self.bag.import_errors, {})
        self.assertEqual(set(self.bag.dags), {"historico_mensual"})

    def test_dependencias_y_limites(self):
        self.assertIsNone(self.dag.schedule)
        self.assertFalse(self.dag.catchup)
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertEqual(self.dag.max_active_tasks, 1)
        self.assertEqual(set(self.dag.task_dict), set(CADENA))
        for index, task_id in enumerate(CADENA):
            tarea = self.dag.get_task(task_id)
            self.assertEqual(tarea.downstream_task_ids, set(CADENA[index + 1:index + 2]))
            self.assertEqual(tarea.upstream_task_ids, set(CADENA[max(0, index - 1):index]))
            self.assertEqual(tarea.retries, 0)
            self.assertEqual(tarea.trigger_rule, "all_success")
            self.assertTrue(tarea.do_xcom_push)
            self.assertFalse(tarea.multiple_outputs)
            self.assertEqual(tarea.store, "/store/historico")
            self.assertEqual(tarea.sbs_dir, "/sources/sbs")

    def test_parametro_obligatorio_36_meses_y_limites(self):
        parametro = self.dag.params.get_param("periodo")
        with self.assertRaises(ParamValidationError):
            deepcopy(parametro).resolve()
        for anio in (2023, 2024, 2025):
            for mes in range(1, 13):
                periodo = f"{anio}-{mes:02d}"
                self.assertEqual(deepcopy(parametro).resolve(periodo), periodo)
        for invalido in (None, True, 202406, "", "2022-12", "2026-01", "2024-00", "2024-13", "2024-6", "2024-06\n", "2024-06-01"):
            with self.subTest(periodo=invalido), self.assertRaises(ParamValidationError):
                deepcopy(parametro).resolve(invalido)

    def test_periodo_invalido_se_rechaza_antes_del_adaptador(self):
        for periodo in (None, False, 202401, "2022-12", "2026-01", "2025-13", "2024-6", "2024-06\n"):
            with self.subTest(periodo=periodo), patch.object(o, "AdaptadorCLI") as crear:
                with self.assertRaises(ValueError):
                    self.tarea("ingest").execute(self.contexto(periodo=periodo))
                crear.assert_not_called()
        self.popen.assert_not_called()

    def test_serializacion_sin_estado_de_procesos(self):
        serializado = DagSerialization.to_dict(self.dag)
        texto = json.dumps(serializado)
        self.assertNotIn("_adaptador", texto)
        restaurado = DagSerialization.from_dict(serializado)
        self.assertEqual(set(restaurado.task_dict), set(CADENA))
        self.assertEqual(restaurado.max_active_runs, 1)

    def test_cadena_cli_json_y_xcom_solo_identificadores_y_hashes(self):
        tokens = {}
        datos = respuestas()
        for index, task_id in enumerate(CADENA):
            with self.subTest(task_id=task_id):
                self.responder(datos[task_id])
                contexto = self.contexto()
                contexto["ti"].xcom_pull.side_effect = lambda task_ids, key: tokens[task_ids]
                token = self.dag.get_task(task_id).execute(contexto)
                campo = "version_id" if task_id in ("publish", "verify_version") else "candidate_id"
                self.assertEqual(token, {campo: datos[task_id][campo], "manifest_sha256": datos[task_id]["manifest_sha256"]})
                tokens[task_id] = token
                if index:
                    contexto["ti"].xcom_pull.assert_called_once_with(task_ids=CADENA[index - 1], key="return_value")
                contexto["ti"].xcom_push.assert_not_called()
        comandos = [llamada.args[0] for llamada in self.popen.call_args_list]
        self.assertEqual(len(comandos), 6)
        self.assertEqual([c[3] for c in comandos], ["ingest", "verify", "rebuild", "verify", "publish", "verify"])
        self.assertEqual(comandos[0][-6:], ["--candidate", "initial", "--month", "2024-06", "--sbs-dir", "/sources/sbs"])
        self.assertEqual(comandos[2][-2:], ["--candidate", INGEST])
        self.assertEqual(comandos[4][-2:], ["--candidate", REBUILD])
        self.assertEqual(comandos[5][-2:], ["--version", tokens["publish"]["version_id"]])
        self.assertNotEqual(VERSION, "v-" + HASH_VERSION)  # Hash de paquete != hash de rebuild.
        self.assertFalse(any("resolve" in c for c in comandos))

    def test_xcom_ausente_alterado_o_con_datos_extra(self):
        for dato in (None, {}, {"candidate_id": "../otro", "manifest_sha256": HASH_INGEST},
                     {"candidate_id": INGEST, "manifest_sha256": "abc"},
                     {"candidate_id": INGEST, "manifest_sha256": HASH_INGEST, "path": "/no/transferir"}):
            with self.subTest(dato=dato), patch.object(o, "AdaptadorCLI") as crear:
                with self.assertRaises(a.ErrorContratoCLI):
                    self.tarea("verify_ingest").execute(self.contexto(dato))
                crear.assert_not_called()

    def test_verify_compara_identificador_y_hash(self):
        for campo, valor in (("candidate_id", "ingest-" + "f" * 32), ("manifest_sha256", "f" * 64)):
            dato = respuestas()["verify_ingest"]
            dato[campo] = valor
            dato["path"] = f"/store/historico/work/{dato['candidate_id']}"
            self.responder(dato)
            with self.subTest(campo=campo), self.assertRaises(a.ErrorContratoCLI):
                self.tarea("verify_ingest").execute(self.contexto({"candidate_id": INGEST, "manifest_sha256": HASH_INGEST}))

    def test_rebuild_compara_hash_del_origen(self):
        dato = respuestas()["rebuild"]
        dato["source_candidate"]["manifest_sha256"] = "f" * 64
        self.responder(dato)
        with self.assertRaisesRegex(a.ErrorContratoCLI, "manifiesto ingerido"):
            self.tarea("rebuild").execute(self.contexto({"candidate_id": INGEST, "manifest_sha256": HASH_INGEST}))

    def test_publish_compara_version_con_rebuild_verificado(self):
        dato = respuestas()["publish"]
        dato["version_id"] = "v-" + "f" * 64
        dato["path"] = f"/store/historico/versions/{dato['version_id']}"
        self.responder(dato)
        with self.assertRaisesRegex(a.ErrorContratoCLI, "manifiesto reconstruido"):
            self.tarea("publish").execute(self.contexto({"candidate_id": REBUILD, "manifest_sha256": HASH_REBUILD}))

    def test_verify_final_no_acepta_otra_version_ni_hash(self):
        for campo, valor in (("version_id", "v-" + "f" * 64), ("manifest_sha256", "f" * 64)):
            dato = respuestas()["verify_version"]
            dato[campo] = valor
            dato["path"] = f"/store/historico/versions/{dato['version_id']}"
            self.responder(dato)
            with self.subTest(campo=campo), self.assertRaises(a.ErrorContratoCLI):
                self.tarea("verify_version").execute(self.contexto({"version_id": VERSION, "manifest_sha256": HASH_VERSION}))

    def test_errores_cli_conservan_codigo_incierto_y_evidencia_sin_reintento(self):
        for codigo in (1, 2, 3, 4):
            with self.subTest(codigo=codigo):
                self.popen.reset_mock()
                self.responder(b"", codigo=codigo, stderr=b"evidencia=/store/diagnostico.json\n")
                with self.assertRaises(a.ErrorCLI) as error:
                    self.tarea("publish").execute(self.contexto({"candidate_id": REBUILD, "manifest_sha256": HASH_REBUILD}))
                self.assertEqual(error.exception.returncode, codigo)
                self.assertIn("/store/diagnostico.json", error.exception.stderr)
                self.assertEqual(self.popen.call_count, 1)

    def test_error_json_no_devuelve_identidad(self):
        for texto in (b"", b"{}\n{}", b"progreso\n{}", b'{"schema_version": 999}'):
            with self.subTest(texto=texto):
                self.responder(texto)
                with self.assertRaises(a.ErrorContratoCLI):
                    self.tarea("ingest").execute(self.contexto())

    def test_on_kill_conecta_cancelar_y_no_devuelve_exito(self):
        tarea = self.tarea("ingest")
        with patch.object(o, "AdaptadorCLI") as crear:
            adaptador = crear.return_value

            def cancelar_y_responder(*args, **kwargs):
                tarea.on_kill()
                return respuestas()["ingest"]

            adaptador.ejecutar.side_effect = cancelar_y_responder
            with self.assertRaises(a.CancelacionCLI):
                tarea.execute(self.contexto())
            adaptador.cancelar.assert_called_once_with()
            self.assertIsNone(tarea._adaptador)

    def test_on_kill_antes_del_arranque(self):
        tarea = self.tarea("ingest")
        tarea.on_kill()
        with patch.object(o, "AdaptadorCLI") as crear:
            with self.assertRaises(a.CancelacionCLI):
                tarea.execute(self.contexto())
            crear.assert_not_called()

    def test_on_kill_durante_constructor_del_adaptador(self):
        tarea = self.tarea("ingest")
        adaptador = MagicMock()
        adaptador.ejecutar.side_effect = a.CancelacionCLI("cancelado antes del subproceso")

        def crear():
            tarea.on_kill()
            return adaptador

        with patch.object(o, "AdaptadorCLI", side_effect=crear):
            with self.assertRaises(a.CancelacionCLI):
                tarea.execute(self.contexto())
            adaptador.cancelar.assert_called_once_with()
            self.assertIsNone(tarea._adaptador)


if __name__ == "__main__":
    unittest.main()
