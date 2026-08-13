"""
MCP Server HTTP — consulta las tablas OFIMA replicadas en PostgreSQL (esquema: backups).
Desplegable en Railway como servicio web.

Claude web: pega la URL de Railway en "Agregar conector personalizado"
Ejemplo: https://tu-app.up.railway.app/mcp
"""

import os
import json
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

mcp = FastMCP("OFIMA Data", stateless_http=True)

PG_SCHEMA = "backups"

AVAILABLE_TABLES = [
    "mvtrade",
    "mtmercia",
    "vseriesutilidad",
    "mvcuadre",
    "vabonos",
    "abocxp",
    "vcxp",
]


def get_conn():
    return psycopg2.connect(
        os.environ["DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


# ── Herramientas MCP ──────────────────────────────────────────────────────────

@mcp.tool()
def list_tables() -> str:
    """Lista las tablas OFIMA disponibles."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = ANY(%s)
        ORDER BY table_name
    """, (PG_SCHEMA, AVAILABLE_TABLES))
    rows = [r["table_name"] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return json.dumps(rows)


@mcp.tool()
def describe_table(table_name: str) -> str:
    """
    Devuelve las columnas y tipos de una tabla OFIMA.

    Args:
        table_name: Nombre de la tabla (mvtrade, mtmercia, vseriesutilidad, mvcuadre, vabonos, abocxp, vcxp)
    """
    if table_name not in AVAILABLE_TABLES:
        return json.dumps({"error": f"Tabla '{table_name}' no permitida."})

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """, (PG_SCHEMA, table_name))
    cols = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return json.dumps(cols, ensure_ascii=False)


@mcp.tool()
def query_table(table_name: str, limit: int = 100, filters: str = "") -> str:
    """
    Consulta filas de una tabla OFIMA con filtros opcionales en SQL WHERE.

    Args:
        table_name: Nombre de la tabla
        limit: Máximo de filas a retornar (default 100, max 1000)
        filters: Condición SQL opcional, ej: "habilitado = 1" o "fecha > '2026-01-01'"
    """
    if table_name not in AVAILABLE_TABLES:
        return json.dumps({"error": f"Tabla '{table_name}' no permitida."})

    limit = min(int(limit), 1000)
    where = f"WHERE {filters}" if filters.strip() else ""
    sql = f'SELECT * FROM "{PG_SCHEMA}"."{table_name}" {where} LIMIT {limit}'

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        cur.close()
        conn.close()
        return json.dumps({"error": str(e)})

    cur.close()
    conn.close()

    def serialize(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    return json.dumps(rows, default=serialize, ensure_ascii=False)


@mcp.tool()
def count_rows(table_name: str, filters: str = "") -> str:
    """
    Cuenta filas de una tabla OFIMA.

    Args:
        table_name: Nombre de la tabla
        filters: Condición SQL opcional, ej: "estado = 'A'"
    """
    if table_name not in AVAILABLE_TABLES:
        return json.dumps({"error": f"Tabla '{table_name}' no permitida."})

    where = f"WHERE {filters}" if filters.strip() else ""
    sql = f'SELECT COUNT(*) AS total FROM "{PG_SCHEMA}"."{table_name}" {where}'

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        result = dict(cur.fetchone())
    except Exception as e:
        cur.close()
        conn.close()
        return json.dumps({"error": str(e)})

    cur.close()
    conn.close()
    return json.dumps(result)


@mcp.tool()
def run_custom_query(sql: str) -> str:
    """
    Ejecuta una consulta SQL SELECT personalizada sobre las tablas OFIMA.
    Las tablas están en el esquema 'backups', ej: SELECT * FROM backups.mvtrade
    Solo se permiten consultas SELECT.

    Args:
        sql: Consulta SQL SELECT
    """
    sql_clean = sql.strip().upper()
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE"]
    if not sql_clean.startswith("SELECT"):
        return json.dumps({"error": "Solo se permiten consultas SELECT."})
    for word in forbidden:
        if word in sql_clean:
            return json.dumps({"error": f"Operación '{word}' no permitida."})

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        cur.close()
        conn.close()
        return json.dumps({"error": str(e)})

    cur.close()
    conn.close()

    def serialize(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    return json.dumps(rows, default=serialize, ensure_ascii=False)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    os.environ.setdefault("FASTMCP_HOST", "0.0.0.0")
    os.environ.setdefault("FASTMCP_PORT", str(port))
    mcp.run(transport="streamable-http")
