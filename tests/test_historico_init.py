import csv
import hashlib
import json
from itertools import product
from pathlib import Path
import subprocess
import sys

import pytest

from src.pipeline import historico
from src.pipeline.validacion import (
    COLUMNAS_HECHOS,
    MESES,
    leer_hechos,
    validar_decisiones,
    validar_features,
)


REPOSITORIO = Path(__file__).resolve().parents[1]


def inventario(raiz):
    return {
        ruta.relative_to(raiz).as_posix(): hashlib.sha256(ruta.read_bytes()).hexdigest()
        for ruta in raiz.rglob("*") if ruta.is_file()
    }


def tabla(ruta):
    with ruta.open(encoding="utf-8", newline="") as archivo:
        return list(csv.reader(archivo))


def escribir_tabla(ruta, filas):
    with ruta.open("w", encoding="utf-8", newline="") as archivo:
        csv.writer(archivo, lineterminator="\n").writerows(filas)


@pytest.fixture(autouse=True)
def referencias_reales_intactas():
    originales = {ruta: (REPOSITORIO / ruta).read_bytes() for ruta in historico.REFERENCIAS}
    yield
    assert {ruta: (REPOSITORIO / ruta).read_bytes() for ruta in originales} == originales


@pytest.fixture
def referencias(tmp_path):
    raiz = tmp_path / "repositorio"
    for nombre in (*historico.REFERENCIAS, historico.MANIFIESTO_REFERENCIAS):
        destino = raiz / nombre
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes((REPOSITORIO / nombre).read_bytes())
    return raiz


def test_manifiesto_versionado(referencias):
    manifiesto = json.loads((referencias / historico.MANIFIESTO_REFERENCIAS).read_bytes())
    assert set(manifiesto) == {"schema_version", "source_commit", "references"}
    assert manifiesto["source_commit"] == "3a07619c5c2f790f68ad8cddb0443eafa997d80d"
    assert {entrada["path"] for entrada in manifiesto["references"]} == set(historico.REFERENCIAS)
    for entrada in manifiesto["references"]:
        assert set(entrada) == {"path", "sha256"}
        assert not Path(entrada["path"]).is_absolute()
        assert ".." not in Path(entrada["path"]).parts
        assert entrada["sha256"] == hashlib.sha256((referencias / entrada["path"]).read_bytes()).hexdigest()


def test_inicializacion_ida_y_vuelta(referencias, tmp_path):
    antes = inventario(referencias)
    store = tmp_path / "historico"
    candidato = historico.inicializar(store, referencias)
    manifiesto = historico.verificar(store, referencias)
    assert manifiesto["status"] == "prepared"
    assert manifiesto["stage"] == "historical-init"
    assert len(manifiesto["artifacts"]) == 80
    assert "manifest.json" not in manifiesto["artifacts"]
    assert manifiesto["artifacts"] == {
        nombre: huella for nombre, huella in inventario(candidato).items()
        if nombre != "manifest.json"
    }
    assert all(not Path(nombre).is_absolute() for nombre in manifiesto["artifacts"])
    for nombre in historico.REFERENCIAS:
        assert (candidato / "references" / nombre).read_bytes() == (referencias / nombre).read_bytes()
    for original, recompuesto in historico.RECOMPUESTOS.items():
        esperado = tabla(referencias / original)
        observado = tabla(candidato / recompuesto)
        assert observado[0] == esperado[0]
        assert sorted(observado[1:]) == sorted(esperado[1:])

    bancos = {"BCP", "BBVA", "Interbank", "Scotiabank", "Mibanco", "Total Banca Múltiple"}
    carteras = {"Consumo", "Hipotecario"}
    indicadores = {"Morosidad", "Créditos Directos Totales", "Tasa Activa MN"}
    assert {ruta.name for ruta in (candidato / "partitions").iterdir()} == set(MESES)
    for mes in MESES:
        sbs = tabla(candidato / f"partitions/{mes}/sbs.csv")
        bcrp = tabla(candidato / f"partitions/{mes}/bcrp.csv")
        assert len(sbs[1:]) == 36
        assert {tuple(fila[:4]) for fila in sbs[1:]} == set(product((mes,), bancos, carteras, indicadores))
        assert len(bcrp[1:]) == 3
        assert {tuple(fila[:4]) for fila in bcrp[1:]} == {
            (mes, "Macro", "Macro", indicador)
            for indicador in ("tasa_referencia", "tipo_cambio", "inflacion")
        }
    assert inventario(referencias) == antes
    assert not (store / "LATEST").exists()
    assert not (store / "versions").exists()


@pytest.mark.parametrize("nombre", historico.REFERENCIAS)
def test_referencia_alterada_se_rechaza(referencias, tmp_path, nombre):
    archivo = referencias / nombre
    archivo.write_bytes(archivo.read_bytes() + b"\n")
    store = tmp_path / "historico"
    with pytest.raises(ValueError, match="Referencia alterada"):
        historico.inicializar(store, referencias)
    assert not store.exists()


@pytest.mark.parametrize("fuente", ("SBS", "BCRP"))
@pytest.mark.parametrize("defecto, mensaje", [
    ("mes_ausente", "Faltan"),
    ("duplicado", "Clave duplicada"),
    ("combinacion", "Clave no esperada"),
    ("fecha_fuera", "Clave no esperada"),
    ("nan", "Número no finito"),
    ("infinito", "Número no finito"),
])
def test_hechos_invalidos(referencias, tmp_path, fuente, defecto, mensaje):
    nombre = historico.SBS if fuente == "SBS" else historico.BCRP
    filas = tabla(referencias / nombre)
    conteo_original = len(filas)
    if defecto == "mes_ausente":
        filas = [filas[0], *[fila for fila in filas[1:] if fila[0] != "2023-01"]]
    elif defecto == "duplicado":
        filas[-1] = filas[1].copy()
    elif defecto == "combinacion":
        filas[1][1] = "Macro" if fuente == "SBS" else "BCP"
    elif defecto == "fecha_fuera":
        filas[1][0] = "2026-01"
    else:
        filas[1][-1] = "NaN" if defecto == "nan" else "Infinity"
    if defecto != "mes_ausente":
        assert len(filas) == conteo_original
    archivo = tmp_path / "datos_invalidos.csv"
    escribir_tabla(archivo, filas)
    with pytest.raises(ValueError, match=mensaje):
        leer_hechos(archivo.read_bytes(), fuente)


def test_esquema_y_orden_columnas(referencias, tmp_path):
    filas = tabla(referencias / historico.SBS)
    filas[0] = list(reversed(COLUMNAS_HECHOS))
    archivo = tmp_path / "esquema.csv"
    escribir_tabla(archivo, filas)
    with pytest.raises(ValueError, match="Esquema incorrecto"):
        leer_hechos(archivo.read_bytes(), "SBS")


@pytest.mark.parametrize("nombre, validar", [
    (historico.FEATURES, validar_features),
    (historico.DECISIONES, validar_decisiones),
])
def test_numeros_no_finitos_en_otras_referencias(referencias, tmp_path, nombre, validar):
    filas = tabla(referencias / nombre)
    campo = "morosidad_t" if nombre == historico.FEATURES else "prob_riesgo"
    filas[1][filas[0].index(campo)] = "NaN"
    archivo = tmp_path / "no_finito.csv"
    escribir_tabla(archivo, filas)
    with pytest.raises(ValueError, match="Número no finito"):
        validar(archivo.read_bytes())


def test_repeticion_no_sobrescribe(referencias, tmp_path):
    store = tmp_path / "historico"
    historico.inicializar(store, referencias)
    antes = inventario(store)
    with pytest.raises(ValueError, match="ya está preparado"):
        historico.inicializar(store, referencias)
    assert inventario(store) == antes


def test_candidato_incompleto_no_se_repara(referencias, tmp_path):
    store = tmp_path / "historico"
    candidato = historico.inicializar(store, referencias)
    (candidato / "partitions/2024-07/bcrp.csv").unlink()
    antes = inventario(store)
    for operacion in (historico.verificar, historico.inicializar):
        with pytest.raises(ValueError, match="Candidato incompleto"):
            operacion(store, referencias)
        assert inventario(store) == antes


def test_fallo_preparacion_no_declara_preparado(referencias, tmp_path, monkeypatch):
    original = historico._escribir_csv
    llamadas = 0

    def fallar(ruta, columnas, filas):
        nonlocal llamadas
        llamadas += 1
        if llamadas == 2:
            ruta.write_text("escritura parcial\n", encoding="utf-8")
            raise OSError("fallo de preparación simulado")
        original(ruta, columnas, filas)

    monkeypatch.setattr(historico, "_escribir_csv", fallar)
    store = tmp_path / "historico"
    with pytest.raises(OSError, match="fallo de preparación"):
        historico.inicializar(store, referencias)
    assert not (store / historico.CANDIDATO / "manifest.json").exists()
    antes = inventario(store)
    with pytest.raises(ValueError, match="Candidato incompleto"):
        historico.inicializar(store, referencias)
    assert inventario(store) == antes


@pytest.mark.parametrize("destino", ("repositorio", "interior", "ancestro", "enlace"))
def test_store_separado_de_referencias(referencias, tmp_path, destino):
    destinos = {
        "repositorio": referencias,
        "interior": referencias / "data" / "historico",
        "ancestro": tmp_path,
        "enlace": tmp_path / "alias",
    }
    if destino == "enlace":
        destinos[destino].symlink_to(referencias, target_is_directory=True)
    antes = inventario(referencias)
    with pytest.raises(ValueError, match="fuera del repositorio"):
        historico.inicializar(destinos[destino], referencias)
    assert inventario(referencias) == antes


def test_hashes_del_candidato_se_verifican(referencias, tmp_path):
    store = tmp_path / "historico"
    candidato = historico.inicializar(store, referencias)
    ruta = candidato / "manifest.json"
    manifiesto = json.loads(ruta.read_bytes())
    manifiesto["artifacts"]["partitions/2023-01/sbs.csv"] = "0" * 64
    ruta.write_text(json.dumps(manifiesto), encoding="utf-8")
    with pytest.raises(ValueError, match="hashes de artefactos diferentes"):
        historico.verificar(store, referencias)


def test_token_modificado_se_detecta(referencias, tmp_path):
    store = tmp_path / "historico"
    candidato = historico.inicializar(store, referencias)
    archivo = candidato / "partitions/2023-01/sbs.csv"
    filas = tabla(archivo)
    filas[1][-1] += "0"
    escribir_tabla(archivo, filas)
    with pytest.raises(ValueError, match="Diferencia en Particiones SBS"):
        historico.verificar(store, referencias)


def test_cli_init_y_verify(tmp_path):
    store = tmp_path / "cli-historico"
    for comando, mensaje in (("init", "Candidato preparado"), ("verify", "Candidato verificado")):
        resultado = subprocess.run(
            [sys.executable, "-B", "-m", "src.pipeline.historico", comando, "--store", str(store)],
            cwd=REPOSITORIO, text=True, capture_output=True
        )
        assert resultado.returncode == 0, resultado.stderr
        assert mensaje in resultado.stdout


def test_cli_exige_store():
    with pytest.raises(SystemExit) as salida:
        historico.main(["init"])
    assert salida.value.code == 2
