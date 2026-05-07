"""
Pipeline ETL - Proyecto Final Python
Procesa ficheros Clientes-YYYY-MM-DD.csv y Tarjetas-YYYY-MM-DD.csv
y los carga en una base de datos MySQL.

Autores: Ken, Laura, Naza
"""

import os
import re
import hashlib
import logging
import logging.handlers
import unicodedata
import pandas as pd
from datetime import datetime
from pathlib import Path


# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
ERRORS_DIR = BASE_DIR / "errors"
LOGS_DIR   = BASE_DIR / "logs"

# La sal se lee de variable de entorno; si no existe, usa un valor por defecto de desarrollo
SALT = os.getenv("ETL_SALT", "salt_proyecto_daw_2025")

for d in (OUTPUT_DIR, ERRORS_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("etl")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")

    # Fichero rotativo: máximo 5 ficheros de 1 MB
    fh = logging.handlers.RotatingFileHandler(
        LOGS_DIR / "etl.log", maxBytes=1_048_576, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)

    # Consola (solo INFO o superior)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logger()


# ─────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────
FILE_PATTERN = re.compile(r"^(Clientes|Tarjetas)-(\d{4}-\d{2}-\d{2})\.csv$")


def remove_accents(text: str) -> str:
    """Elimina tildes y diacríticos de una cadena."""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def capitalize_name(name: str) -> str:
    """
    Capitaliza un nombre correctamente, respetando guiones.
    Ejemplo: ANA-MARIA → Ana-Maria
    """
    if not name:
        return name
    parts = name.split("-")
    return "-".join(p.capitalize() for p in parts)


def validate_dni(dni: str) -> bool:
    """
    Valida un DNI español: 8 dígitos seguidos de la letra de control correcta.
    La letra se calcula con el módulo 23.
    """
    dni = re.sub(r"[\s\-]", "", dni).upper()
    if not re.match(r"^\d{8}[A-Z]$", dni):
        return False
    letras = "TRWAGMYFPDXBNJZSQVHLCKE"
    return letras[int(dni[:8]) % 23] == dni[-1]


def validate_phone(phone: str) -> bool:
    """Valida que el teléfono tenga entre 9 y 15 dígitos (admite prefijo internacional)."""
    digits = re.sub(r"[\s\+\-\(\)]", "", phone)
    return bool(re.match(r"^\d{9,15}$", digits))


def clean_phone(phone: str) -> str:
    """Elimina espacios y símbolos del teléfono, dejando solo dígitos."""
    return re.sub(r"[\s\+\-\(\)]", "", phone)


def validate_email(email: str) -> bool:
    """Validación básica de formato de correo electrónico."""
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email))


def hash_value(value: str) -> str:
    """Genera un hash SHA-256 con sal para anonimizar datos sensibles."""
    salted = SALT + value
    return hashlib.sha256(salted.encode("utf-8")).hexdigest()


def mask_card(number: str) -> str:
    """
    Enmascara un número de tarjeta mostrando solo los últimos 4 dígitos.
    Ejemplo: 4532 1234 5678 9012 → XXXX-XXXX-XXXX-9012
    """
    digits = re.sub(r"[\s\-]", "", number)
    if len(digits) >= 4:
        return f"XXXX-XXXX-XXXX-{digits[-4:]}"
    return "XXXX-XXXX-XXXX-XXXX"


# ─────────────────────────────────────────────
# DETECCIÓN DE FICHEROS
# ─────────────────────────────────────────────
def discover_files(source_dir: Path) -> dict:
    """
    Recorre el directorio fuente y clasifica los ficheros según el patrón
    Clientes-YYYY-MM-DD.csv / Tarjetas-YYYY-MM-DD.csv.
    Los ficheros que no cumplan el patrón se ignoran y se registra un aviso.
    """
    found = {"Clientes": [], "Tarjetas": []}
    for f in source_dir.iterdir():
        if not f.is_file():
            continue
        m = FILE_PATTERN.match(f.name)
        if m:
            tipo, fecha = m.group(1), m.group(2)
            found[tipo].append((fecha, f))
            logger.info(f"Fichero detectado: {f.name}  (tipo={tipo}, fecha={fecha})")
        else:
            logger.warning(f"Fichero ignorado (no cumple el patrón esperado): {f.name}")
    return found


# ─────────────────────────────────────────────
# ETL: CLIENTES
# ─────────────────────────────────────────────
def process_clientes(filepath: Path, fecha: str) -> pd.DataFrame:
    logger.info(f"[Clientes] Leyendo: {filepath.name}")

    # Intentamos UTF-8 primero; si falla, probamos latin-1
    try:
        df = pd.read_csv(filepath, sep=";", encoding="utf-8", dtype=str, on_bad_lines="warn")
    except UnicodeDecodeError:
        logger.warning("[Clientes] UTF-8 falló, reintentando con latin-1")
        df = pd.read_csv(filepath, sep=";", encoding="latin-1", dtype=str, on_bad_lines="warn")

    logger.info(f"[Clientes] Filas leídas: {len(df)}")

    # Renombrado de columnas para consistencia en la BD
    # Documentado aquí: "Cod cliente" → cod_cliente (espacio eliminado)
    col_map = {
        "Cod cliente": "cod_cliente",
        "nombre":      "nombre",
        "apellido1":   "apellido1",
        "apellido2":   "apellido2",
        "dni":         "dni",
        "correo":      "correo",
        "telefono":    "telefono",
    }
    df.rename(columns=col_map, inplace=True)

    # Trim global: elimina espacios al inicio y al final de todas las celdas
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Reemplazar cadenas vacías por NaN
    df.replace("", pd.NA, inplace=True)

    # ── Limpieza y normalización ─────────────────
    df["nombre"]    = df["nombre"].apply(
        lambda x: remove_accents(capitalize_name(x)) if pd.notna(x) else x
    )
    df["apellido1"] = df["apellido1"].apply(
        lambda x: remove_accents(capitalize_name(x)) if pd.notna(x) else x
    )
    df["apellido2"] = df["apellido2"].apply(
        lambda x: remove_accents(capitalize_name(x)) if pd.notna(x) else x
    )
    df["correo"]    = df["correo"].apply(
        lambda x: x.lower().strip() if pd.notna(x) else x
    )
    df["dni"]       = df["dni"].apply(
        lambda x: re.sub(r"[\s\-]", "", x).upper() if pd.notna(x) else x
    )
    df["telefono"]  = df["telefono"].apply(
        lambda x: clean_phone(x) if pd.notna(x) else x
    )

    # ── Columnas de validación ───────────────────
    df["DNI_OK"]      = df["dni"].apply(lambda x: "Y" if pd.notna(x) and validate_dni(x) else "N")
    df["DNI_KO"]      = df["dni"].apply(lambda x: "N" if pd.notna(x) and validate_dni(x) else "Y")
    df["Telefono_OK"] = df["telefono"].apply(lambda x: "Y" if pd.notna(x) and validate_phone(x) else "N")
    df["Telefono_KO"] = df["telefono"].apply(lambda x: "N" if pd.notna(x) and validate_phone(x) else "Y")
    df["Correo_OK"]   = df["correo"].apply(lambda x: "Y" if pd.notna(x) and validate_email(x) else "N")
    df["Correo_KO"]   = df["correo"].apply(lambda x: "N" if pd.notna(x) and validate_email(x) else "Y")

    # ── Anonimización DNI ────────────────────────
    # dni_masked: para el CSV de salida (trazabilidad parcial)
    # dni_hash:   SHA-256 + sal (para la BD, jamás el DNI en claro)
    df["dni_masked"] = df["dni"].apply(
        lambda x: x[:3] + "****" + x[-1] if pd.notna(x) and len(x) > 4 else x
    )
    df["dni_hash"] = df["dni"].apply(
        lambda x: hash_value(x) if pd.notna(x) else pd.NA
    )

    # ── Validación de campos obligatorios ────────
    mask_ok  = df["cod_cliente"].notna() & df["correo"].notna()
    rejected = df[~mask_ok].copy()
    df       = df[mask_ok].copy()

    if not rejected.empty:
        rejected["motivo_rechazo"] = "cod_cliente o correo nulo"
        out_err = ERRORS_DIR / f"rows_rejected_clientes_{fecha}.csv"
        rejected.to_csv(out_err, sep=";", index=False, encoding="utf-8")
        logger.warning(f"[Clientes] {len(rejected)} filas rechazadas → {out_err.name}")

    # ── Guardar CSV limpio ───────────────────────
    out_path = OUTPUT_DIR / f"Clientes-{fecha}.cleaned.csv"
    df.to_csv(out_path, sep=";", index=False, encoding="utf-8")
    logger.info(f"[Clientes] {len(df)} filas guardadas → {out_path.name}")

    return df


# ─────────────────────────────────────────────
# ETL: TARJETAS
# ─────────────────────────────────────────────
def process_tarjetas(filepath: Path, fecha: str) -> pd.DataFrame:
    logger.info(f"[Tarjetas] Leyendo: {filepath.name}")

    try:
        df = pd.read_csv(filepath, sep=";", encoding="utf-8", dtype=str, on_bad_lines="warn")
    except UnicodeDecodeError:
        logger.warning("[Tarjetas] UTF-8 falló, reintentando con latin-1")
        df = pd.read_csv(filepath, sep=";", encoding="latin-1", dtype=str, on_bad_lines="warn")

    logger.info(f"[Tarjetas] Filas leídas: {len(df)}")

    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df.replace("", pd.NA, inplace=True)

    # ── Anonimización tarjeta ────────────────────
    # Nunca almacenamos el número en claro ni el CVV
    df["numero_tarjeta_raw"]    = df["numero_tarjeta"].apply(
        lambda x: re.sub(r"[\s\-]", "", x) if pd.notna(x) else x
    )
    df["numero_tarjeta_masked"] = df["numero_tarjeta"].apply(
        lambda x: mask_card(x) if pd.notna(x) else x
    )
    df["numero_tarjeta_hash"]   = df["numero_tarjeta_raw"].apply(
        lambda x: hash_value(x) if pd.notna(x) else pd.NA
    )
    df["cvv_hash"] = df["cvv"].apply(
        lambda x: hash_value(x) if pd.notna(x) else pd.NA
    )

    # Eliminamos columnas sensibles del DataFrame
    df.drop(columns=["numero_tarjeta", "numero_tarjeta_raw", "cvv"], inplace=True)

    # ── Validación ───────────────────────────────
    mask_ok  = df["cod_cliente"].notna()
    rejected = df[~mask_ok].copy()
    df       = df[mask_ok].copy()

    if not rejected.empty:
        rejected["motivo_rechazo"] = "cod_cliente nulo"
        out_err = ERRORS_DIR / f"rows_rejected_tarjetas_{fecha}.csv"
        rejected.to_csv(out_err, sep=";", index=False, encoding="utf-8")
        logger.warning(f"[Tarjetas] {len(rejected)} filas rechazadas → {out_err.name}")

    # ── Guardar CSV limpio ───────────────────────
    out_path = OUTPUT_DIR / f"Tarjetas-{fecha}.cleaned.csv"
    df.to_csv(out_path, sep=";", index=False, encoding="utf-8")
    logger.info(f"[Tarjetas] {len(df)} filas guardadas → {out_path.name}")

    return df


# ─────────────────────────────────────────────
# BASE DE DATOS (MySQL)
# ─────────────────────────────────────────────
def get_engine():
    """
    Crea el motor de conexión a MySQL usando variables de entorno.
    Variables necesarias: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
    """
    from sqlalchemy import create_engine

    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "3306")
    user     = os.getenv("DB_USER", "etl_user")
    password = os.getenv("DB_PASSWORD", "etl_password")
    db_name  = os.getenv("DB_NAME", "etl_db")

    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(url)


def ensure_tables(engine):
    """
    Crea las tablas en MySQL si no existen todavía.
    Si ya existen, no hace nada (no sobreescribe datos).
    """
    from sqlalchemy import text

    ddl_clientes = """
    CREATE TABLE IF NOT EXISTS clientes (
        cod_cliente   VARCHAR(20)  NOT NULL,
        nombre        VARCHAR(100),
        apellido1     VARCHAR(100),
        apellido2     VARCHAR(100),
        dni_masked    VARCHAR(20),
        dni_hash      VARCHAR(64),
        correo        VARCHAR(200),
        telefono      VARCHAR(20),
        DNI_OK        CHAR(1),
        DNI_KO        CHAR(1),
        Telefono_OK   CHAR(1),
        Telefono_KO   CHAR(1),
        Correo_OK     CHAR(1),
        Correo_KO     CHAR(1),
        fecha_carga   VARCHAR(10)  NOT NULL,
        PRIMARY KEY (cod_cliente, fecha_carga)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    ddl_tarjetas = """
    CREATE TABLE IF NOT EXISTS tarjetas (
        cod_cliente           VARCHAR(20)  NOT NULL,
        numero_tarjeta_masked VARCHAR(25),
        numero_tarjeta_hash   VARCHAR(64),
        fecha_exp             VARCHAR(10),
        cvv_hash              VARCHAR(64),
        fecha_carga           VARCHAR(10)  NOT NULL,
        PRIMARY KEY (cod_cliente, numero_tarjeta_hash, fecha_carga)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    with engine.connect() as conn:
        conn.execute(text(ddl_clientes))
        conn.execute(text(ddl_tarjetas))
        conn.commit()

    logger.info("[DB] Tablas verificadas (creadas si no existían)")


def insert_data(df: pd.DataFrame, table: str, engine, fecha: str):
    """
    Inserta las filas del DataFrame en la tabla indicada.
    Añade la columna fecha_carga para trazabilidad.
    """
    df = df.copy()
    df["fecha_carga"] = fecha

    cols_tabla = {
        "clientes": [
            "cod_cliente", "nombre", "apellido1", "apellido2",
            "dni_masked", "dni_hash", "correo", "telefono",
            "DNI_OK", "DNI_KO", "Telefono_OK", "Telefono_KO",
            "Correo_OK", "Correo_KO", "fecha_carga"
        ],
        "tarjetas": [
            "cod_cliente", "numero_tarjeta_masked", "numero_tarjeta_hash",
            "fecha_exp", "cvv_hash", "fecha_carga"
        ],
    }

    cols = [c for c in cols_tabla[table] if c in df.columns]
    df[cols].to_sql(table, con=engine, if_exists="append", index=False)
    logger.info(f"[DB] {len(df)} filas insertadas en '{table}'")


# ─────────────────────────────────────────────
# ORQUESTADOR PRINCIPAL
# ─────────────────────────────────────────────
def run_pipeline():
    logger.info("=" * 55)
    logger.info("INICIO PIPELINE ETL")
    logger.info("=" * 55)
    start = datetime.now()

    files = discover_files(INPUT_DIR)

    try:
        engine = get_engine()
        ensure_tables(engine)
        use_db = True
    except Exception as e:
        logger.warning(f"No se pudo conectar a la BD: {e}")
        logger.warning("El pipeline continuará pero NO insertará en BD.")
        engine   = None
        use_db   = False

    for fecha, fpath in files["Clientes"]:
        df_c = process_clientes(fpath, fecha)
        if use_db:
            insert_data(df_c, "clientes", engine, fecha)

    for fecha, fpath in files["Tarjetas"]:
        df_t = process_tarjetas(fpath, fecha)
        if use_db:
            insert_data(df_t, "tarjetas", engine, fecha)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"FIN PIPELINE ETL — tiempo total: {elapsed:.1f}s")
    logger.info("=" * 55)


if __name__ == "__main__":
    run_pipeline()
