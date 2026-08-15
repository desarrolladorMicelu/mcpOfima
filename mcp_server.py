"""
MCP Server HTTP — consulta las tablas OFIMA replicadas en PostgreSQL (esquema: backups).
Desplegable en Railway como servicio web.

Claude web: pega la URL de Railway en "Agregar conector personalizado"
Ejemplo: https://tu-app.up.railway.app/mcp

Auth: GitHub OAuth (GitHubProvider de FastMCP)
Requiere variables de entorno:
  GITHUB_CLIENT_ID     — Client ID de tu GitHub OAuth App
  GITHUB_CLIENT_SECRET — Client Secret de tu GitHub OAuth App
  BASE_URL             — URL pública del servidor, ej: https://tu-app.up.railway.app
"""

import os
import json
import threading
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from dotenv import load_dotenv
from fastmcp import FastMCP
from fastmcp.server.auth.providers.github import GitHubProvider

load_dotenv()

# ── GitHub OAuth ──────────────────────────────────────────────────────────────
_base_url = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

auth_provider = GitHubProvider(
    client_id=os.environ["GITHUB_CLIENT_ID"],
    client_secret=os.environ["GITHUB_CLIENT_SECRET"],
    base_url=_base_url,
)

mcp = FastMCP("OFIMA Data", auth=auth_provider)

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

@mcp.tool
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


@mcp.tool
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


@mcp.tool
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


@mcp.tool
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


@mcp.tool
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
    # ── Cron interno: corre replicate.py todos los días a las 2 AM UTC ───────
    def replication_loop():
        log = logging.getLogger("replicator")
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        log.info("Scheduler de replicación iniciado. Corre a las 02:00 UTC diario.")
        while True:
            now = datetime.now(timezone.utc)
            # Calcular segundos hasta las 02:00 UTC del próximo día
            next_run = now.replace(hour=0, minute=12, second=0, microsecond=0)
            if now >= next_run:
                next_run = next_run.replace(day=next_run.day + 1)
            wait_seconds = (next_run - now).total_seconds()
            log.info(f"Próxima replicación en {wait_seconds/3600:.1f}h ({next_run.strftime('%Y-%m-%d %H:%M')} UTC)")
            time.sleep(wait_seconds)
            try:
                from replicate import main as run_replication
                log.info("Iniciando replicación OFIMA → PostgreSQL ...")
                run_replication()
            except Exception as e:
                log.error(f"Error en replicación: {e}")

    t = threading.Thread(target=replication_loop, daemon=True)
    t.start()

    # ── MCP server ─────────────────────────────────────────────────────────────
    port = int(os.environ.get("PORT", 8000))
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=port,
    )
