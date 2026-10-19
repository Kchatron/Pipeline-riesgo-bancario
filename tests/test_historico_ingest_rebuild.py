"""Conexión ingesta/reconstrucción: fuentes nuevas, golden exacto y trazabilidad."""

from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import shutil

import pandas as pd
import pytest

from src.data import bcrp_extractor, sbs_extractor
from src.pipeline import historico as h, ingesta
from src.pipeline.validacion_rebuild import verificar_cortes_referencia


REPO = Path(__file__).resolve().parents[1]
CLAVES = ["fecha_id", "banco_id", "tipo_credito_id"]
MES = "2023-01"


def inventario(raiz):
    return {ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
            for ruta in raiz.rglob("*") if ruta.is_file()}


def cargar(ruta):
    return json.loads(ruta.read_bytes())


def guardar(ruta, datos):
    ruta.write_text(json.dumps(datos, ensure_ascii=False, allow_nan=False), encoding="utf-8")


def features(ruta):
    columnas = pd.read_csv(ruta, nrows=0).columns
    tipos = {campo: "float64" for campo in columnas[3:-1]}
    return pd.read_csv(ruta, float_precision="round_trip", dtype={**tipos, "riesgo_alto": "int64"})


@pytest.fixture(autouse=True)
def referencias_intactas():
    rutas = [REPO / ruta for ruta in h.REFERENCIAS] + list((REPO / "data/raw/SBS").iterdir())
    antes = {ruta: hashlib.sha256(ruta.read_bytes()).hexdigest() for ruta in rutas if ruta.is_file()}
    yield
    assert {ruta: hashlib.sha256(ruta.read_bytes()).hexdigest() for ruta in antes} == antes


@pytest.fixture
def entorno(tmp_path, monkeypatch, request):
    store = tmp_path / "store"
    inicial = h.inicializar(store)
    antes = inventario(inicial)
    carpeta = tmp_path / "excel"
    carpeta.mkdir()
    modo = getattr(request, "param", "controlados")
    reales = modo == "reales"
    nombres = ("B-2362-2023-01.xls", "B-2334-2023-01.xls", "Tasas-2023-01.xlsx")
    for nombre in nombres:
        if reales:
            shutil.copyfile(REPO / "data/raw/SBS" / nombre, carpeta / nombre)
        else:
            (carpeta / nombre).write_bytes(b"Entradas controladas para funciones SBS simuladas")
    sbs = pd.read_csv(REPO / h.SBS, dtype=str, keep_default_na=False)
    if not reales:
        def simular(indicador):
            def extraer(ruta, mes):
                assert ruta.is_relative_to(store / "work") and ruta.is_file()
                filas = sbs[sbs.fecha_id.eq(mes) & sbs.indicador_id.eq(indicador)].copy()
                # Valor equivalente, token diferente: no debe imponerse el contrato textual de init.
                mascara = filas.banco_id.eq("Total Banca Múltiple") if modo != "tokens_largos" else filas.banco_id.notna()
                filas.loc[mascara, "valor"] += "0"
                return filas
            return extraer
        for nombre, indicador in (("extraer_morosidad", "Morosidad"),
                                  ("extraer_creditos_directos", "Créditos Directos Totales"),
                                  ("extraer_tasas_activas", "Tasa Activa MN")):
            monkeypatch.setattr(sbs_extractor, nombre, simular(indicador))
    macro = pd.read_csv(REPO / h.BCRP, dtype=str)
    valores = macro[macro.fecha_id.eq(MES)].set_index("indicador_id").valor
    payload = {"config": {"series": [{"name": codigo} for codigo in bcrp_extractor.SERIES_BCRP]},
               "periods": [{"name": "Ene.2023", "values": [valores[nombre] for nombre in bcrp_extractor.SERIES_BCRP.values()]}]}
    solicitudes = []

    def urlopen(request):
        solicitudes.append(request.full_url)
        return io.BytesIO(json.dumps(payload).encode("utf-8"))

    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", urlopen)
    yield {"store": store, "initial": inicial, "excel": carpeta, "payload": payload,
           "solicitudes": solicitudes, "reales": reales}
    assert inventario(inicial) == antes


@pytest.fixture
def ingerido(entorno):
    origen = ingesta.ingerir(entorno["store"], "initial", MES, entorno["excel"])
    antes = inventario(origen)
    yield origen
    assert inventario(origen) == antes


def sin_extraccion(monkeypatch):
    def prohibido(*args, **opciones):
        pytest.fail("rebuild/verify no puede extraer ni consultar la API")
    monkeypatch.setattr(bcrp_extractor.urllib.request, "urlopen", prohibido)
    for nombre in ("extraer_morosidad", "extraer_creditos_directos", "extraer_tasas_activas"):
        monkeypatch.setattr(sbs_extractor, nombre, prohibido)


@pytest.mark.parametrize("entorno", ["controlados", "reales"], indirect=True)
def test_flujo_cli_ingest_rebuild_verify_usa_consolidados(entorno, ingerido, monkeypatch, capsys):
    constructor = h.construir_dataset_analitico
    auditor = h.auditar_p75
    usadas, poblaciones = [], []
    fuente_sbs = (ingerido / h.RECOMPUESTOS[h.SBS]).read_bytes()

    def observar(datos, sbs):
        assert sbs == fuente_sbs
        poblaciones.append(sbs)
        return auditor(datos, sbs)

    def construir(**opciones):
        usadas.append(opciones)
        for fuente, argumento in ((h.SBS, "ruta_sbs"), (h.BCRP, "ruta_bcrp")):
            ruta = opciones[argumento]
            assert ruta.is_file() and not ruta.is_relative_to(ingerido)
            assert ruta.read_bytes() == (ingerido / h.RECOMPUESTOS[fuente]).read_bytes()
            assert not ruta.samefile(ingerido / h.RECOMPUESTOS[fuente])
        return constructor(**opciones)

    monkeypatch.setattr(h, "construir_dataset_analitico", construir)
    monkeypatch.setattr(h, "auditar_p75", observar)
    sin_extraccion(monkeypatch)
    assert h.main(["rebuild", "--store", str(entorno["store"]), "--candidate", ingerido.name]) == 0
    salida = capsys.readouterr().out
    identificador = next(linea.split(": ", 1)[1] for linea in salida.splitlines() if linea.startswith("Candidato reconstruido:"))
    nuevo = entorno["store"] / "work" / identificador
    assert len(usadas) == len(poblaciones) == 1
    assert h.main(["verify", "--store", str(entorno["store"]), "--candidate", identificador]) == 0
    manifiesto = cargar(nuevo / "manifest.json")
    assert manifiesto["source_candidate"] == {"id": ingerido.name,
        "manifest_sha256": hashlib.sha256((ingerido / "manifest.json").read_bytes()).hexdigest()}
    assert len(manifiesto["artifacts"]) == 89
    assert manifiesto["schema_version"] == 2
    assert manifiesto["publication_base"] is None
    assert manifiesto["artifacts"] == {ruta: huella for ruta, huella in inventario(nuevo).items() if ruta != "manifest.json"}
    for ruta in cargar(ingerido / "manifest.json")["artifacts"]:
        assert (nuevo / ruta).read_bytes() == (ingerido / ruta).read_bytes()
    if not entorno["reales"]:
        informe = cargar(nuevo / ingesta.INFORME)
        assert any(fila["numeric_equal"] and not fila["tokens_equal"] for fila in informe["comparisons"] if fila["dataset"] == "SBS")
    resultado = features(nuevo / h.FEATURES_RECONSTRUIDAS).set_index(CLAVES).sort_index()
    referencia = features(REPO / h.FEATURES).set_index(CLAVES).sort_index()
    pd.testing.assert_frame_equal(resultado, referencia, check_exact=True)
    sbs = pd.read_csv(nuevo / h.RECOMPUESTOS[h.SBS], float_precision=None)
    train = sbs[sbs.indicador_id.eq("Morosidad") & sbs.banco_id.ne("Total Banca Múltiple")
                & sbs.fecha_id.between("2023-01", "2024-12")]
    cortes = train.groupby(CLAVES[1:]).valor.quantile(.75, interpolation="linear")
    referencia_sbs = pd.read_csv(REPO / h.SBS, float_precision=None)
    train_ref = referencia_sbs[referencia_sbs.indicador_id.eq("Morosidad")
                              & referencia_sbs.banco_id.ne("Total Banca Múltiple")
                              & referencia_sbs.fecha_id.between("2023-01", "2024-12")]
    cortes_ref = train_ref.groupby(CLAVES[1:]).valor.quantile(.75, interpolation="linear")
    auditoria = cargar(nuevo / h.AUDITORIA_P75)
    assert len(auditoria["panels"]) == 10
    for panel in auditoria["panels"]:
        clave = panel["banco_id"], panel["tipo_credito_id"]
        assert panel["count"] == 24
        assert panel["observed_cut"] == panel["expected_cut"] == cortes.loc[clave] == cortes_ref.loc[clave]
    assert len(entorno["solicitudes"]) == 1


def test_bcrp_aceptado_en_ingest_bloqueado_por_golden(entorno, monkeypatch, capsys):
    valores = entorno["payload"]["periods"][0]["values"]
    valores[0] = str(Decimal(valores[0]) + Decimal("5e-10"))
    origen = ingesta.ingerir(entorno["store"], "initial", MES, entorno["excel"])
    antes = inventario(origen)
    informe = cargar(origen / ingesta.INFORME)
    assert informe["status"] == "matched"
    assert any(Decimal(fila["difference"]) != 0 and fila["accepted"]
               for fila in informe["comparisons"] if fila["dataset"] == "BCRP")
    sin_extraccion(monkeypatch)
    assert h.main(["rebuild", "--store", str(entorno["store"]), "--candidate", origen.name]) == 1
    fallido = next(ruta for ruta in (entorno["store"] / "work").iterdir() if ruta.name.startswith("rebuild-"))
    diagnostico = cargar(fallido / h.DIAGNOSTICO_REBUILD)
    assert diagnostico["status"] == "failed" and diagnostico["path"] == str(fallido)
    assert "sin tolerancias" in diagnostico["error"] and "max_abs" in diagnostico["error"]
    assert "tipo_cambio" in diagnostico["error"] and "ejemplos" in diagnostico["error"]
    assert str(fallido) in capsys.readouterr().err
    assert not (fallido / "manifest.json").exists()
    for ruta in (h.RECOMPUESTOS[h.BCRP], ingesta.INFORME, ingesta.PROCEDENCIA):
        assert (fallido / ruta).read_bytes() == (origen / ruta).read_bytes()
    assert inventario(origen) == antes
    candidato = features(fallido / h.FEATURES_RECONSTRUIDAS).set_index(CLAVES).sort_index()
    referencia = features(REPO / h.FEATURES).set_index(CLAVES).sort_index()
    delta = (candidato - referencia).abs()
    assert delta.tipo_cambio_lag2.max() > 0 and delta.tipo_cambio_lag2.max() < 1e-9
    with pytest.raises(ValueError, match="incompleto"):
        h.verificar(entorno["store"], candidato=fallido.name)


@pytest.mark.parametrize("entorno", ["tokens_largos"], indirect=True)
def test_sbs_equivalente_decimal_rechazado_si_cambia_float64(entorno, ingerido):
    informe = cargar(ingerido / ingesta.INFORME)
    assert informe["status"] == "matched"
    assert all(fila["numeric_equal"] for fila in informe["comparisons"] if fila["dataset"] == "SBS")
    with pytest.raises(ValueError, match="morosidad_lag2"):
        h.reconstruir(entorno["store"], candidato=ingerido.name)
    nuevo = next(ruta for ruta in (entorno["store"] / "work").iterdir() if ruta.name.startswith("rebuild-"))
    assert not (nuevo / "manifest.json").exists()
    assert "sin tolerancias" in cargar(nuevo / h.DIAGNOSTICO_REBUILD)["error"]


@pytest.mark.parametrize("artefacto", [h.RECOMPUESTOS[h.SBS], ingesta.INFORME, ingesta.PROCEDENCIA,
                                      h.AUDITORIA_P75, h.FEATURES_RECONSTRUIDAS])
def test_verify_detecta_alteraciones_copia_aun_con_hash_actualizado(entorno, ingerido, monkeypatch, artefacto):
    nuevo = h.reconstruir(entorno["store"], candidato=ingerido.name)
    ruta = nuevo / artefacto
    if artefacto == h.AUDITORIA_P75:
        datos = cargar(ruta)
        datos["panels"][0]["observed_cut"] += 1
        guardar(ruta, datos)
    elif artefacto == ingesta.PROCEDENCIA:
        datos = cargar(ruta)
        datos["sbs"][0]["source_uri"] = "file:///otro/origen.xls"
        guardar(ruta, datos)
    elif artefacto == ingesta.INFORME:
        datos = cargar(ruta)
        datos["comparisons"][0]["difference"] = "123"
        guardar(ruta, datos)
    else:
        tabla = pd.read_csv(ruta, dtype=str, keep_default_na=False)
        campo = "valor" if artefacto == h.RECOMPUESTOS[h.SBS] else "tipo_cambio"
        tabla.loc[0, campo] = str(Decimal(tabla.loc[0, campo]) + 1)
        tabla.to_csv(ruta, index=False)
    manifiesto = cargar(nuevo / "manifest.json")
    huella = hashlib.sha256(ruta.read_bytes()).hexdigest()
    manifiesto["artifacts"][artefacto] = huella
    for resultado in (*manifiesto["inputs"].values(), *manifiesto["results"].values()):
        if resultado["path"] == artefacto:
            resultado["sha256"] = huella
    guardar(nuevo / "manifest.json", manifiesto)
    sin_extraccion(monkeypatch)
    with pytest.raises(ValueError):
        h.verificar(entorno["store"], candidato=nuevo.name)


@pytest.mark.parametrize("defecto", ["incompleto", "blocked", "hash", "cadena"])
def test_origen_no_admitido_no_inicia_reconstruccion(entorno, ingerido, defecto):
    marcador = ingerido / "manifest.json"
    antes_bytes = marcador.read_bytes()
    restaurar = None
    if defecto == "incompleto":
        ruta = ingerido / ingesta.INFORME
        restaurar = ruta, ruta.read_bytes()
        ruta.unlink()
    elif defecto == "blocked":
        datos = cargar(marcador)
        datos["status"] = "blocked"
        guardar(marcador, datos)
    elif defecto == "hash":
        datos = cargar(marcador)
        datos["artifacts"][h.RECOMPUESTOS[h.SBS]] = "0" * 64
        guardar(marcador, datos)
    else:
        reconstruido = h.reconstruir(entorno["store"], candidato=ingerido.name)
    candidato = reconstruido.name if defecto == "cadena" else ingerido.name
    antes = inventario(entorno["store"])
    try:
        with pytest.raises((ValueError, OSError)):
            h.reconstruir(entorno["store"], candidato=candidato)
        assert inventario(entorno["store"]) == antes
    finally:
        marcador.write_bytes(antes_bytes)
        if restaurar:
            restaurar[0].write_bytes(restaurar[1])


def test_verify_contrasta_hash_del_manifiesto_directo(entorno, ingerido):
    nuevo = h.reconstruir(entorno["store"], candidato=ingerido.name)
    ruta = ingerido / "manifest.json"
    original = ruta.read_bytes()
    ruta.write_bytes(original + b"\n")
    try:
        h.verificar(entorno["store"], candidato=ingerido.name)
        with pytest.raises(ValueError, match="Hash del manifiesto"):
            h.verificar(entorno["store"], candidato=nuevo.name)
    finally:
        ruta.write_bytes(original)


@pytest.mark.parametrize("fallo", ["constructor", "observador", "csv", "manifest"])
def test_fallo_conserva_diagnostico_sin_manifest(entorno, ingerido, monkeypatch, fallo):
    constructor = h.construir_dataset_analitico
    escribir = h.json.dump

    def construir(**opciones):
        if fallo == "constructor":
            raise OSError("fallo controlado del constructor")
        resultado = constructor(**opciones)
        if fallo == "csv":
            datos = resultado.copy(deep=True)
            datos.loc[0, "tipo_cambio"] += 1
            datos.to_csv(opciones["ruta_salida"], index=False)
        return resultado

    def observar(datos, sbs):
        raise ValueError("fallo controlado del observador")

    def escribir_con_fallo(datos, archivo, **opciones):
        escribir(datos, archivo, **opciones)
        if datos.get("stage") == "historical-rebuild" and datos.get("status") == "prepared":
            raise OSError("fallo controlado al escribir el manifiesto final")

    monkeypatch.setattr(h, "construir_dataset_analitico", construir)
    if fallo == "observador":
        monkeypatch.setattr(h, "auditar_p75", observar)
    elif fallo == "manifest":
        monkeypatch.setattr(h.json, "dump", escribir_con_fallo)
    with pytest.raises((ValueError, OSError)):
        h.reconstruir(entorno["store"], candidato=ingerido.name)
    nuevo = next(ruta for ruta in (entorno["store"] / "work").iterdir() if ruta.name.startswith("rebuild-"))
    assert not (nuevo / "manifest.json").exists()
    diagnostico = cargar(nuevo / h.DIAGNOSTICO_REBUILD)
    assert diagnostico["error"] and diagnostico["source_candidate"]["id"] == ingerido.name
    assert diagnostico["path"] == str(nuevo)


def test_contraste_p75_referencia_rechaza_cortes_distintos(entorno, ingerido):
    nuevo = h.reconstruir(entorno["store"], candidato=ingerido.name)
    auditoria = cargar(nuevo / h.AUDITORIA_P75)
    auditoria["panels"][0]["expected_cut"] += 1
    auditoria["panels"][0]["observed_cut"] += 1
    with pytest.raises(ValueError, match="cortes de referencia"):
        verificar_cortes_referencia(auditoria, (REPO / h.SBS).read_bytes())
