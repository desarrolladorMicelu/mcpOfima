"""
Replicación automática de tablas OFIMA → Supabase (PostgreSQL)
Ejecutar: python replicate.py
Cron Railway: 0 2 * * *  (todos los días a las 2:00 AM)
"""

import os
import sys
import time
import logging
import pyodbc
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

MAX_RETRIES  = 3
BACKOFF_BASE = 10  # segundos


# ── Consultas OFIMA ───────────────────────────────────────────────────────────
# Las fechas se calculan dinámicamente al momento de ejecutar.

def build_queries():
    year      = datetime.now().year
    date_from = f"{year}-01-01"
    date_to   = f"{year + 1}-01-01"

    return [
        # (nombre_destino_supabase, query_origen_ofima)
        (
            "mvtrade",
            f"""
            SELECT
                RTRIM(m.TIPODCTO)   AS tipodcto,
                RTRIM(m.NRODCTO)    AS nrodcto,
                RTRIM(m.NIT)        AS nit,
                RTRIM(m.NOMBRE)     AS nombre,
                RTRIM(m.PRODUCTO)   AS producto,
                RTRIM(m.VENDEDOR)   AS vendedor,
                RTRIM(m.BODEGA)     AS bodega,
                m.FECHA             AS fecha,
                m.FHCOMPRA          AS fhcompra,
                m.FECMOD            AS fecmod,
                m.CANTIDAD          AS cantidad,
                m.VLRVENTA          AS vlrventa,
                m.COSTO             AS costo,
                m.DESCUENTO         AS descuento,
                m.IVA               AS iva,
                RTRIM(m.PASSWORDIN) AS passwordin,
                RTRIM(m.ORIGEN)     AS origen
            FROM dbo.Mvtrade AS m WITH (NOLOCK)
            WHERE m.FHCOMPRA >= '{date_from}'
              AND m.FHCOMPRA <  '{date_to}'
            ORDER BY m.FHCOMPRA DESC
            """
        ),
        (
            "mtmercia",
            """
            SELECT
                RTRIM(m.CODIGO)     AS codigo,
                RTRIM(m.DESCRIPCIO) AS descripcio,
                RTRIM(m.CODLINEA)   AS codlinea,
                RTRIM(m.CODSBLIN)   AS codsblin,
                RTRIM(m.CODGRUPO)   AS codgrupo,
                RTRIM(m.CLASIFICA1) AS clasifica1,
                RTRIM(m.CLASIFICA2) AS clasifica2,
                m.IVA               AS iva,
                m.HABILITADO        AS habilitado,
                RTRIM(m.UBICACION)  AS ubicacion,
                RTRIM(m.UNIDADMED)  AS unidadmed,
                RTRIM(m.TIPOINV)    AS tipoinv
            FROM dbo.MtMercia AS m WITH (NOLOCK)
            ORDER BY m.CODIGO
            """
        ),
        (
            "vseriesutilidad",
            f"""
            SELECT
                RTRIM(v.Producto)       AS producto,
                RTRIM(v.Serie)          AS serie,
                RTRIM(v.Referencia)     AS referencia,
                RTRIM(v.Tipo_Documento) AS tipo_documento,
                v.Fecha_Inicial         AS fecha_inicial,
                RTRIM(v.Nit)            AS nit,
                v.Valor                 AS valor,
                RTRIM(v.Documento)      AS documento
            FROM dbo.VSeriesUtilidad AS v WITH (NOLOCK)
            WHERE v.Fecha_Inicial BETWEEN '{date_from}' AND '{year}-12-31'
            ORDER BY v.Fecha_Inicial DESC
            """
        ),
        (
            "mvcuadre",
            f"""
            SELECT
                CONVERT(DATE, mc.FECHAMVTO)           AS fecha,
                RTRIM(mc.TIPODCTO)                    AS tipodcto,
                RTRIM(mc.DCTO)                        AS dcto,
                RTRIM(mc.TIPODCTO) + RTRIM(mc.DCTO)  AS documento,
                RTRIM(mc.MEDIOPAG)                    AS mediopag,
                RTRIM(mc.BANCO)                       AS banco,
                RTRIM(mc.BANCODEST)                   AS bancodest,
                mc.VALOR                              AS valor,
                RTRIM(mc.NIT)                         AS nit,
                RTRIM(mc.PASSWORDIN)                  AS passwordin,
                RTRIM(mc.ORIGEN)                      AS origen,
                RTRIM(mc.TIPODCTOFA)                  AS tipodctofa
            FROM dbo.MvCuadre AS mc WITH (NOLOCK)
            WHERE mc.FECHAMVTO >= '{date_from}'
              AND mc.FECHAMVTO <  '{date_to}'
              AND mc.VALOR IS NOT NULL
              AND mc.VALOR <> 0
            ORDER BY mc.FECHAMVTO DESC
            """
        ),
        (
            "vabonos",
            f"""
            SELECT
                RTRIM(v.TIPODCTO)                    AS tipodcto,
                RTRIM(v.DCTO)                        AS dcto,
                RTRIM(v.TIPODCTO) + RTRIM(v.DCTO)   AS documento,
                CONVERT(DATE, v.FECHA)               AS fecha,
                RTRIM(v.NIT)                         AS nit,
                RTRIM(v.TIPODCTOCA)                  AS tipodctoca,
                v.VALOR                              AS valor,
                RTRIM(v.BANCO)                       AS banco,
                RTRIM(v.CONCEPTO)                    AS concepto,
                RTRIM(v.NOTA)                        AS nota,
                RTRIM(v.PASSWORDIN)                  AS passwordin
            FROM dbo.VABONOS AS v WITH (NOLOCK)
            WHERE v.FECHA >= '{date_from}'
              AND v.FECHA <  '{date_to}'
              AND v.VALOR IS NOT NULL
              AND v.VALOR <> 0
            ORDER BY v.FECHA DESC
            """
        ),
        (
            "abocxp",
            f"""
            SELECT
                RTRIM(a.TIPODCTO)                        AS tipodcto,
                RTRIM(a.DCTO)                            AS dcto,
                RTRIM(a.TIPODCTO) + RTRIM(a.DCTO)       AS documento,
                CONVERT(DATE, a.FECHA)                   AS fecha,
                RTRIM(a.NIT)                             AS nit,
                RTRIM(a.BENEFICIA)                       AS beneficia,
                RTRIM(a.BANCO)                           AS banco,
                RTRIM(a.BANCOTRAS)                       AS bancotras,
                RTRIM(a.TIPODCTOCP)                      AS tipodctocp,
                RTRIM(a.FORMAPAGO)                       AS formapago,
                a.VALOR                                  AS valor,
                RTRIM(a.PASSWORDIN)                      AS passwordin,
                RTRIM(a.NOTA)                            AS nota,
                RTRIM(a.CONCEPTO)                        AS concepto
            FROM dbo.ABOCXP AS a WITH (NOLOCK)
            WHERE a.FECHA >= '{date_from}'
              AND a.FECHA <  '{date_to}'
              AND a.VALOR IS NOT NULL
              AND a.VALOR <> 0
            ORDER BY a.FECHA DESC
            """
        ),
        (
            "vcxp",
            f"""
            SELECT
                RTRIM(c.Tipodcto)       AS tipodcto,
                RTRIM(c.NroDcto)        AS nrodcto,
                CONVERT(DATE, c.fecha)  AS fecha,
                c.Fhvencim              AS fhvencim,
                RTRIM(c.nit)            AS nit,
                RTRIM(c.CliNombre)      AS clinombre,
                c.bruto                 AS bruto,
                c.Descuento             AS descuento,
                c.IvaBruto              AS ivabruto,
                c.Deuda                 AS deuda,
                c.Pagado                AS pagado,
                RTRIM(c.mediopag)       AS mediopag,
                RTRIM(c.passwordin)     AS passwordin,
                RTRIM(c.Origen)         AS origen,
                RTRIM(c.ciudad)         AS ciudad,
                RTRIM(c.canal)          AS canal
            FROM dbo.VCXP AS c WITH (NOLOCK)
            WHERE c.fecha >= '{date_from}'
              AND c.fecha <  '{date_to}'
            ORDER BY c.fecha DESC
            """
        ),
    ]


# ── Conexiones ────────────────────────────────────────────────────────────────

def get_sqlserver_conn():
    conn_str = (
        f"DRIVER={{{os.environ['SQLSERVER_DRIVER']}}};"
        f"SERVER={os.environ['SQLSERVER_HOST']};"
        f"DATABASE={os.environ['SQLSERVER_DB']};"
        f"UID={os.environ['SQLSERVER_USER']};"
        f"PWD={os.environ['SQLSERVER_PASSWORD']};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str)


def get_postgres_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ── Lógica de replicación ─────────────────────────────────────────────────────

PG_SCHEMA = "backups"


def replicate_table(src_cursor, dst_conn, dst_table: str, query: str):
    """
    Ejecuta la query en OFIMA y sobrescribe la tabla en el esquema backups de PostgreSQL.
    Siempre hace DROP + CREATE para garantizar esquema fresco.
    """
    log.info(f"  Ejecutando query para '{PG_SCHEMA}.{dst_table}' ...")
    src_cursor.arraysize = 2000  # fetch en bloques de 2000
    src_cursor.execute(query)

    # fetchmany en bucle para garantizar que se traen todas las filas
    # independientemente del tamaño del resultado
    rows = []
    while True:
        batch = src_cursor.fetchmany(2000)
        if not batch:
            break
        rows.extend(batch)

    columns = [desc[0].lower() for desc in src_cursor.description]
    col_types = [desc[1] for desc in src_cursor.description]

    log.info(f"  {len(rows)} filas obtenidas")

    if not rows:
        log.warning(f"  '{dst_table}' no devolvió filas. Se crea tabla vacía.")

    # Mapeo de tipos Python → PostgreSQL
    pg_type_map = {
        str:      "TEXT",
        int:      "BIGINT",
        float:    "DOUBLE PRECISION",
        bool:     "BOOLEAN",
        bytes:    "BYTEA",
        datetime: "TIMESTAMP",
    }

    def to_pg_type(col_type):
        return pg_type_map.get(col_type, "TEXT")

    col_defs = ", ".join(
        f'"{col}" {to_pg_type(col_types[i])}'
        for i, col in enumerate(columns)
    )

    full_table = f'"{PG_SCHEMA}"."{dst_table}"'

    with dst_conn.cursor() as cur:
        # Garantizar que el esquema existe
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}"')
        cur.execute(f'DROP TABLE IF EXISTS {full_table}')
        cur.execute(f'CREATE TABLE {full_table} ({col_defs})')

        if rows:
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f'INSERT INTO {full_table} VALUES ({placeholders})'
            batch_size = 500
            for i in range(0, len(rows), batch_size):
                batch = [tuple(r) for r in rows[i: i + batch_size]]
                psycopg2.extras.execute_batch(cur, insert_sql, batch)

        dst_conn.commit()

    log.info(f"  ✓ '{PG_SCHEMA}.{dst_table}' replicada ({len(rows)} filas)")
    return len(rows)


def replicate_with_retry(src_cursor, dst_conn, dst_table, query):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return replicate_table(src_cursor, dst_conn, dst_table, query)
        except Exception as e:
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            log.error(f"  ✗ Intento {attempt}/{MAX_RETRIES} fallido para '{dst_table}': {e}")
            if attempt < MAX_RETRIES:
                log.info(f"  Reintentando en {wait}s ...")
                time.sleep(wait)
            else:
                log.error(f"  ✗ '{dst_table}' falló tras {MAX_RETRIES} intentos. Se continúa.")
                return None


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    start = datetime.now()
    log.info("=" * 60)
    log.info(f"Inicio de replicación OFIMA → Supabase: {start}")
    log.info(f"Año replicado: {start.year}")
    log.info("=" * 60)

    queries = build_queries()
    results = {}

    try:
        src_conn = get_sqlserver_conn()
        log.info("✓ Conexión SQL Server establecida")
    except Exception as e:
        log.error(f"✗ No se pudo conectar a SQL Server: {e}")
        sys.exit(1)

    try:
        dst_conn = get_postgres_conn()
        log.info("✓ Conexión Supabase/PostgreSQL establecida")
    except Exception as e:
        log.error(f"✗ No se pudo conectar a Supabase: {e}")
        src_conn.close()
        sys.exit(1)

    src_cursor = src_conn.cursor()

    for dst_table, query in queries:
        log.info(f"\n── Tabla: {dst_table} ──")
        # Cursor fresco por cada tabla para evitar contaminación de estado
        cur = src_conn.cursor()
        count = replicate_with_retry(cur, dst_conn, dst_table, query)
        cur.close()
        results[dst_table] = count

    src_conn.close()
    dst_conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    log.info("\n" + "=" * 60)
    log.info("RESUMEN")
    log.info("=" * 60)
    for table, count in results.items():
        status = f"{count} filas" if count is not None else "FALLÓ"
        log.info(f"  {table:<25} {status}")
    log.info(f"\nTiempo total: {elapsed:.1f}s")
    log.info("=" * 60)

    failed = [t for t, c in results.items() if c is None]
    if failed:
        log.error(f"Tablas con error: {failed}")
        sys.exit(1)

    log.info("Replicación completada exitosamente.")


if __name__ == "__main__":
    main()
