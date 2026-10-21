"""Contrato del adaptador con procesos simulados: sin Airflow, Docker, Excel ni red."""

from copy import deepcopy
import json
import multiprocessing
import signal
from unittest.mock import MagicMock

import pytest

from src.orquestacion import adaptador_cli as a


INGEST = "ingest-" + "a" * 32
REBUILD = "rebuild-" + "b" * 32
VERSION = "v-" + "c" * 64
HUELLA = "d" * 64
STORE = "/store/historico"


def resultado(operacion, candidato=None, version=None, actual=False):
    es_version = version is not None or operacion == "publish"
    identificador = (version or VERSION) if es_version else (
        INGEST if operacion == "ingest" else REBUILD if operacion == "rebuild" else candidato or "initial")
    datos = {"schema_version": 1, "operation": operacion, "manifest_sha256": HUELLA,
             "path": f"{STORE}/{'versions' if es_version else 'work'}/{identificador}",
             "version_id" if es_version else "candidate_id": identificador}
    if operacion == "publish":
        datos.update(status="already-current" if actual else "published", already_current=actual)
    else:
        etapa = "version" if es_version else ("init" if identificador == "initial" else identificador.split("-")[0])
        datos.update(stage="historical-" + etapa, status={"version": "complete", "init": "prepared",
                                                        "ingest": "sources-validated", "rebuild": "prepared"}[etapa])
        if etapa in ("ingest", "rebuild"):
            origen = candidato if operacion == "rebuild" else "initial"
            datos["source_candidate"] = {"id": origen, "manifest_sha256": HUELLA}
        if etapa == "ingest":
            datos["month"] = "2024-06"
    return datos


@pytest.fixture
def proceso(monkeypatch):
    hijo = MagicMock(pid=12345, returncode=0)
    hijo.communicate.return_value = (json.dumps(resultado("verify")).encode(), b"progreso\nevidencia: /store/diagnostico.json\n")
    popen = MagicMock(return_value=hijo)
    monkeypatch.setattr(a.subprocess, "Popen", popen)
    return hijo, popen


@pytest.mark.parametrize("operacion,opciones,actual", [
    ("ingest", {"candidato": "initial", "mes": "2024-06", "sbs_dir": "/sources/sbs"}, False),
    ("rebuild", {"candidato": "initial"}, False),
    ("rebuild", {"candidato": INGEST}, False),
    ("verify", {}, False),
    ("verify", {"candidato": INGEST}, False),
    ("verify", {"candidato": REBUILD}, False),
    ("verify", {"version": VERSION}, False),
    ("publish", {"candidato": REBUILD}, False),
    ("publish", {"candidato": REBUILD}, True),
])
def test_exito_y_comando_explicito(proceso, capsys, operacion, opciones, actual):
    hijo, popen = proceso
    esperado = resultado(operacion, opciones.get("candidato"), opciones.get("version"), actual)
    hijo.communicate.return_value = (json.dumps(esperado).encode(), b"progreso separado\n")
    assert a.AdaptadorCLI().ejecutar(operacion, store=STORE, **opciones) == esperado
    argumentos = popen.call_args.args[0]
    assert argumentos[:6] == ["/opt/pipeline-python", "-m", "src.pipeline.historico", operacion, "--store", STORE]
    assert argumentos.count("--json") == 1
    if operacion == "ingest":
        assert argumentos[-4:] == ["--month", "2024-06", "--sbs-dir", "/sources/sbs"]
    if operacion == "verify" and "version" in opciones:
        assert argumentos[-2:] == ["--version", VERSION]
    assert popen.call_args.kwargs == {"cwd": "/opt/project", "start_new_session": True,
                                    "stdout": a.subprocess.PIPE, "stderr": a.subprocess.PIPE}
    assert popen.call_count == 1  # No resolve ni segunda consulta a LATEST.
    salida = capsys.readouterr()
    assert salida.out == "" and salida.err == "progreso separado\n"


@pytest.mark.parametrize("codigo", [1, 2, 3, 4, 127, -signal.SIGTERM])
def test_codigo_no_cero_precede_json_y_conserva_evidencia(proceso, codigo, capsys):
    hijo, _ = proceso
    hijo.returncode = codigo
    hijo.communicate.return_value = (b"NO ES JSON", "fase=latest-visible; diagnóstico=/store/fallo.json\n".encode())
    with pytest.raises(a.ErrorCLI) as error:
        a.AdaptadorCLI().ejecutar("publish", store=STORE, candidato=REBUILD)
    assert error.value.returncode == codigo
    assert error.value.stdout == "NO ES JSON"
    assert error.value.stderr == "fase=latest-visible; diagnóstico=/store/fallo.json\n"
    assert capsys.readouterr().err == error.value.stderr


@pytest.mark.parametrize("texto", ["", "progreso\n{}", "{}\n{}", "[]", "null", '{"schema_version": 1, "schema_version": 1}', '{"x": NaN}'])
def test_rechaza_stdout_fuera_de_contrato(proceso, texto):
    hijo, _ = proceso
    hijo.communicate.return_value = (texto.encode(), b"")
    with pytest.raises(a.ErrorContratoCLI):
        a.AdaptadorCLI().ejecutar("verify", store=STORE)


@pytest.mark.parametrize("campo,valor", [
    ("schema_version", 2), ("schema_version", True), ("operation", "resolve"),
    ("candidate_id", "../../escape"), ("candidate_id", INGEST),
    ("manifest_sha256", "abc"), ("path", "/otro/work/initial"),
    ("stage", "historical-ingest"), ("status", "failed"), ("extra", "no previsto"),
])
def test_rechaza_identidad_estado_esquema_y_ruta(proceso, campo, valor):
    hijo, _ = proceso
    datos = resultado("verify")
    datos[campo] = valor
    hijo.communicate.return_value = (json.dumps(datos).encode(), b"")
    with pytest.raises(a.ErrorContratoCLI):
        a.AdaptadorCLI().ejecutar("verify", store=STORE)


@pytest.mark.parametrize("cambio", ["mes", "origen", "hash_origen", "campo_ausente", "prefijo"])
def test_ingesta_valida_mes_origen_y_campos(proceso, cambio):
    hijo, _ = proceso
    datos = resultado("ingest")
    if cambio == "mes":
        datos["month"] = "2025-02"
    elif cambio == "origen":
        datos["source_candidate"]["id"] = INGEST
    elif cambio == "hash_origen":
        datos["source_candidate"]["manifest_sha256"] = "no hash"
    elif cambio == "campo_ausente":
        del datos["source_candidate"]
    else:
        datos["candidate_id"] = REBUILD
        datos["path"] = f"{STORE}/work/{REBUILD}"
    hijo.communicate.return_value = (json.dumps(datos).encode(), b"")
    with pytest.raises(a.ErrorContratoCLI):
        a.AdaptadorCLI().ejecutar("ingest", store=STORE, candidato="initial", mes="2024-06", sbs_dir="/sources/sbs")


@pytest.mark.parametrize("cambio", ["version", "already_current", "estado"])
def test_publicacion_no_acepta_identidad_o_estado_incoherente(proceso, cambio):
    hijo, _ = proceso
    datos = resultado("publish", REBUILD)
    datos[{"version": "version_id", "already_current": "already_current", "estado": "status"}[cambio]] = {
        "version": "v-escape", "already_current": 1, "estado": "already-current"}[cambio]
    hijo.communicate.return_value = (json.dumps(datos).encode(), b"")
    with pytest.raises(a.ErrorContratoCLI):
        a.AdaptadorCLI().ejecutar("publish", store=STORE, candidato=REBUILD)


def test_verify_version_no_sustituye_la_solicitada(proceso):
    hijo, _ = proceso
    hijo.communicate.return_value = (json.dumps(resultado("verify", version=VERSION)).encode(), b"")
    with pytest.raises(a.ErrorContratoCLI, match="otro candidato o versión"):
        a.AdaptadorCLI().ejecutar("verify", store=STORE, version="v-" + "e" * 64)


@pytest.mark.parametrize("codigo", [0, 4])
def test_utf8_invalido_no_oculta_codigo_de_publicacion(proceso, codigo):
    hijo, _ = proceso
    hijo.returncode = codigo
    hijo.communicate.return_value = (b"\xff", b"evidencia conservada\n")
    with pytest.raises(a.ErrorContratoCLI if codigo == 0 else a.ErrorCLI) as error:
        a.AdaptadorCLI().ejecutar("publish", store=STORE, candidato=REBUILD)
    if codigo == 4:
        assert error.value.returncode == 4
        assert error.value.stderr == "evidencia conservada\n"


def test_rebuild_rechaza_otro_origen(proceso):
    hijo, _ = proceso
    hijo.communicate.return_value = (json.dumps(resultado("rebuild", "initial")).encode(), b"")
    with pytest.raises(a.ErrorContratoCLI, match="otro candidato"):
        a.AdaptadorCLI().ejecutar("rebuild", store=STORE, candidato=INGEST)


def test_lanzador_ausente_conserva_error_original(proceso):
    _, popen = proceso
    original = FileNotFoundError("/opt/pipeline-python")
    popen.side_effect = original
    with pytest.raises(FileNotFoundError) as error:
        a.AdaptadorCLI().ejecutar("verify", store=STORE)
    assert error.value is original


@pytest.mark.parametrize("operacion,opciones", [
    ("resolve", {}), ("init", {}), ("verify", {"candidato": "../initial"}),
    ("verify", {"candidato": "initial", "version": VERSION}), ("verify", {"version": "v-abc"}),
    ("ingest", {"candidato": "initial", "mes": "2026-01", "sbs_dir": "/sources/sbs"}),
    ("ingest", {"candidato": "initial", "mes": "2024-6", "sbs_dir": "/sources/sbs"}),
    ("ingest", {"candidato": INGEST, "mes": "2024-06", "sbs_dir": "/sources/sbs"}),
    ("rebuild", {"candidato": REBUILD}), ("rebuild", {}),
    ("publish", {"candidato": "initial"}), ("publish", {"version": VERSION}),
    ("verify", {"mes": "2024-06"}), ("verify", {"sbs_dir": "/sources/sbs"}),
])
def test_argumentos_invalidos_no_inician_proceso(proceso, operacion, opciones):
    _, popen = proceso
    with pytest.raises(ValueError):
        a.AdaptadorCLI().ejecutar(operacion, store=STORE, **opciones)
    popen.assert_not_called()


@pytest.mark.parametrize("store", ["relativo", "/store/../otro"])
def test_store_absoluto_sin_escape(proceso, store):
    with pytest.raises(ValueError):
        a.AdaptadorCLI().ejecutar("verify", store=store)
    proceso[1].assert_not_called()


def test_cancelar_antes_impide_crear_subproceso(proceso):
    adaptador = a.AdaptadorCLI()
    adaptador.cancelar()
    with pytest.raises(a.CancelacionCLI):
        adaptador.ejecutar("verify", store=STORE)
    proceso[1].assert_not_called()


def test_cancelacion_mata_grupo_y_no_emite_exito_aunque_hijo_devuelva_json(proceso, monkeypatch):
    hijo, _ = proceso
    adaptador = a.AdaptadorCLI()
    matar = MagicMock()
    monkeypatch.setattr(a.os, "killpg", matar)
    salida = deepcopy(hijo.communicate.return_value)

    def terminar():
        adaptador.cancelar()
        return salida

    hijo.communicate.side_effect = terminar
    with pytest.raises(a.CancelacionCLI, match="LATEST puede haber cambiado"):
        adaptador.ejecutar("verify", store=STORE)
    matar.assert_called_once_with(hijo.pid, signal.SIGKILL)


@pytest.mark.parametrize("interrupcion", [KeyboardInterrupt(), SystemExit(143)])
def test_interrupcion_espera_hijo_y_propaga_excepcion_original(proceso, monkeypatch, interrupcion, capsys):
    hijo, _ = proceso
    matar = MagicMock()
    monkeypatch.setattr(a.os, "killpg", matar)
    hijo.communicate.side_effect = [interrupcion, (b"", b"evidencia tras cancelar\n")]
    with pytest.raises(type(interrupcion)) as error:
        a.AdaptadorCLI().ejecutar("verify", store=STORE)
    assert error.value is interrupcion
    matar.assert_called_once_with(hijo.pid, signal.SIGKILL)
    assert hijo.communicate.call_count == 2
    assert capsys.readouterr().err == "evidencia tras cancelar\n"


def test_grupo_ya_terminado_no_oculta_interrupcion(proceso, monkeypatch):
    hijo, _ = proceso
    monkeypatch.setattr(a.os, "killpg", MagicMock(side_effect=ProcessLookupError()))
    hijo.communicate.side_effect = [KeyboardInterrupt(), (b"", b"")]
    with pytest.raises(KeyboardInterrupt):
        a.AdaptadorCLI().ejecutar("verify", store=STORE)


def test_instancia_no_repite_operacion(proceso):
    adaptador = a.AdaptadorCLI()
    adaptador.ejecutar("verify", store=STORE)
    with pytest.raises(RuntimeError, match="una instancia"):
        adaptador.ejecutar("verify", store=STORE)
    assert proceso[1].call_count == 1


@pytest.mark.parametrize("momento", ["arranque", "validacion"])
def test_cancelacion_reentrante_en_mismo_hilo_no_bloquea_ni_devuelve_exito(monkeypatch, momento):
    # Un proceso de prueba permite detectar el deadlock sin dejar un hilo colgado.
    # Los únicos procesos reales son estos workers; Popen y killpg son simulados.
    contexto = multiprocessing.get_context("fork")
    lectura, escritura = contexto.Pipe(duplex=False)

    def comprobar():
        lectura.close()
        eventos = []
        adaptador = a.AdaptadorCLI()
        hijo = MagicMock(pid=12345, returncode=0)

        def esperar():
            eventos.append("esperado")
            return json.dumps(resultado("verify")).encode(), b""

        def crear(*args, **kwargs):
            eventos.append("arranque")
            if momento == "arranque":
                adaptador.cancelar()  # Mismo hilo, antes de asignar _proceso.
            eventos.append("creado")
            return hijo

        validar_original = a._validar

        def validar(*args, **kwargs):
            if momento == "validacion":
                adaptador.cancelar()  # Mismo hilo dentro de la validación.
            return validar_original(*args, **kwargs)

        hijo.communicate.side_effect = esperar
        monkeypatch.setattr(a.subprocess, "Popen", crear)
        monkeypatch.setattr(a, "_validar", validar)
        monkeypatch.setattr(a.os, "killpg", lambda pid, senal: eventos.append([pid, int(senal)]))
        try:
            adaptador.ejecutar("verify", store=STORE)
        except a.CancelacionCLI:
            escritura.send({"cancelado": True, "eventos": eventos, "esperas": hijo.communicate.call_count,
                            "proceso_liberado": adaptador._proceso is None})
        except BaseException as error:
            escritura.send({"error": repr(error)})
        else:
            escritura.send({"cancelado": False})
        finally:
            escritura.close()

    worker = contexto.Process(target=comprobar)
    worker.start()
    escritura.close()
    try:
        # Es un límite para detectar bloqueo, no una pausa para ordenar la carrera.
        assert lectura.poll(5), f"cancelar() quedó bloqueado durante {momento}"
        informe = lectura.recv()
        worker.join(timeout=5)
        assert worker.exitcode == 0
        assert informe.get("cancelado") is True, informe
        assert informe["esperas"] == 1 and informe["proceso_liberado"]
        senal = [12345, int(signal.SIGKILL)]
        assert informe["eventos"] == (["arranque", "creado", senal, "esperado"] if momento == "arranque"
                                      else ["arranque", "creado", "esperado", senal])
    finally:
        if worker.is_alive():
            worker.kill()
        worker.join(timeout=5)
        lectura.close()
