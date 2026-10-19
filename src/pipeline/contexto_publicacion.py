"""Captura de base mediante bloqueo compartido; nunca escribe LATEST."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from contextlib import contextmanager


CONTEXTO = "audit/publication-base.json"


def _pares_unicos(pares):
    resultado = {}
    for clave, valor in pares:
        if clave in resultado:
            raise ValueError(f"Clave JSON duplicada: {clave}.")
        resultado[clave] = valor
    return resultado


def leer_json(contenido):
    def no_finito(valor):
        raise ValueError(f"Constante JSON no finita: {valor}.")
    return json.loads(contenido, object_pairs_hook=_pares_unicos, parse_constant=no_finito)


def validar_identidad(identidad):
    if not isinstance(identidad, dict) or set(identidad) != {"version_id", "manifest_sha256"}:
        raise ValueError("Identidad de LATEST inválida.")
    if (not isinstance(identidad["version_id"], str)
            or not re.fullmatch(r"v-[0-9a-f]{64}", identidad["version_id"])
            or not isinstance(identidad["manifest_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", identidad["manifest_sha256"])):
        raise ValueError("Identificador o hash de LATEST inválido.")


def validar_contexto(contenido):
    datos = leer_json(contenido)
    if (not isinstance(datos, dict) or set(datos) != {"schema_version", "base"}
            or type(datos["schema_version"]) is not int or datos["schema_version"] != 1):
        raise ValueError("Contexto de publicación inválido.")
    if datos["base"] is not None:
        validar_identidad(datos["base"])
    return datos


def campos_contexto(candidato, esquema):
    if type(esquema) is not int or esquema not in (1, 2):
        raise ValueError("Esquema de candidato no admitido.")
    if esquema == 1:
        return {}
    ruta = candidato / CONTEXTO
    if ruta.is_symlink() or ruta.parent.is_symlink() or not ruta.is_file():
        raise ValueError("Falta contexto de publicación válido; no se asigna retrospectivamente.")
    contenido = ruta.read_bytes()
    datos = validar_contexto(contenido)
    return {"publication_base": datos["base"],
            "publication_context": {"path": CONTEXTO, "sha256": hashlib.sha256(contenido).hexdigest()}}


def guardar_contexto(candidato, contenido):
    validar_contexto(contenido)
    ruta = candidato / CONTEXTO
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("xb") as archivo:
        archivo.write(contenido)
        archivo.flush()
        os.fsync(archivo.fileno())


@contextmanager
def bloqueo_publicacion(store, exclusivo=False):
    """Un solo inode estable, compartido por captura y publicación."""
    store = Path(store)
    bloqueo = store / ".publish.lock"
    opciones = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    try:
        descriptor = os.open(bloqueo, opciones, 0o600)
    except OSError as error:
        raise ValueError("No se puede abrir .publish.lock sin seguir enlaces.") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(".publish.lock debe ser un archivo regular.")
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusivo else fcntl.LOCK_SH)
        inode = bloqueo.lstat()
        abierto = os.fstat(descriptor)
        if (inode.st_dev, inode.st_ino) != (abierto.st_dev, abierto.st_ino):
            raise ValueError("El archivo estable .publish.lock cambió durante la captura.")
        yield descriptor
    finally:
        os.close(descriptor)


def leer_latest(store, ancla):
    """Lee el puntero una vez; solo la ausencia del propio LATEST devuelve None."""
    store = Path(store)
    try:
        puntero = os.open(store / "LATEST", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValueError("LATEST inválido o enlace simbólico no permitido.") from error
    with os.fdopen(puntero, "rb") as archivo:
        if not stat.S_ISREG(os.fstat(archivo.fileno()).st_mode):
            raise ValueError("LATEST debe ser un archivo regular.")
        identidad = leer_json(archivo.read())
    validar_identidad(identidad)
    from src.pipeline.versiones import archivo_paquete, verificar_version
    datos = verificar_version(store, identidad["version_id"], ancla)
    version = store / "versions" / identidad["version_id"]
    manifiesto = archivo_paquete(version, "manifest.json").read_bytes()
    if hashlib.sha256(manifiesto).hexdigest() != identidad["manifest_sha256"]:
        raise ValueError("Hash de manifiesto diferente del indicado por LATEST.")
    if datos["version_id"] != identidad["version_id"]:
        raise ValueError("LATEST no coincide con el identificador del paquete.")
    return identidad


def capturar_contexto(store, ancla):
    """Devuelve bytes del contexto; null solo si falta el propio LATEST."""
    with bloqueo_publicacion(store):
        base = leer_latest(store, ancla)
        return (json.dumps({"schema_version": 1, "base": base}, sort_keys=True) + "\n").encode("utf-8")
