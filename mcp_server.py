"""
MCP Server HTTP — consulta tablas OFIMA directamente en SQL Server.
Desplegable en Railway como servicio web.

Claude web: pega la URL de Railway en "Agregar conector personalizado"
Ejemplo: https://tu-app.up.railway.app/mcp

Auth: GitHub OAuth (GitHubProvider de FastMCP)
Requiere variables de entorno:
  SQLSERVER_HOST       — IP o hostname del SQL Server
  SQLSERVER_DB         — Nombre de la base de datos OFIMA
  SQLSERVER_USER       — Usuario de solo lectura
  SQLSERVER_PASSWORD   — Contraseña
  SQLSERVER_DRIVER     — Driver ODBC (ej: ODBC Driver 17 for SQL Server)
  GITHUB_CLIENT_ID     — Client ID de tu GitHub OAuth App
  GITHUB_CLIENT_SECRET — Client Secret de tu GitHub OAuth App
  BASE_URL             — URL pública del servidor, ej: https://tu-app.up.railway.app
"""

import os
import json
import logging
import pyodbc
from datetime import datetime
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


def row_to_dict(cursor, row):
    """Convierte una fila pyodbc a diccionario usando los nombres de columna."""
    columns = [desc[0].lower() for desc in cursor.description]
    return dict(zip(columns, row))


def serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


# ── Herramientas MCP ──────────────────────────────────────────────────────────

@mcp.tool
def list_tables() -> str:
    """Lista las tablas OFIMA disponibles."""
    return json.dumps(AVAILABLE_TABLES)


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
    cursor = conn.cursor()
    try:
        # Obtener una fila para extraer los metadatos de columnas
        cursor.execute(f"SELECT TOP 0 * FROM dbo.{table_name}")
        cols = [
            {"column_name": desc[0], "data_type": str(desc[1])}
            for desc in cursor.description
        ]
    except Exception as e:
        cursor.close()
        conn.close()
        return json.dumps({"error": str(e)})
    finally:
        cursor.close()
        conn.close()

    return json.dumps(cols, ensure_ascii=False)


@mcp.tool
def query_table(table_name: str, limit: int = 100, filters: str = "") -> str:
    """
    Consulta filas de una tabla OFIMA con filtros opcionales en SQL WHERE.

    Args:
        table_name: Nombre de la tabla
        limit: Máximo de filas a retornar (default 100, max 1000)
        filters: Condición SQL opcional, ej: "HABILITADO = 1" o "FECHA > '2026-01-01'"
    """
    if table_name not in AVAILABLE_TABLES:
        return json.dumps({"error": f"Tabla '{table_name}' no permitida."})

    limit = min(int(limit), 1000)
    where = f"WHERE {filters}" if filters.strip() else ""
    sql = f"SELECT TOP {limit} * FROM dbo.{table_name} WITH (NOLOCK) {where}"

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = [row_to_dict(cursor, row) for row in cursor.fetchall()]
    except Exception as e:
        cursor.close()
        conn.close()
        return json.dumps({"error": str(e)})
    finally:
        cursor.close()
        conn.close()

    return json.dumps(rows, default=serialize, ensure_ascii=False)


@mcp.tool
def count_rows(table_name: str, filters: str = "") -> str:
    """
    Cuenta filas de una tabla OFIMA.

    Args:
        table_name: Nombre de la tabla
        filters: Condición SQL opcional, ej: "HABILITADO = 1"
    """
    if table_name not in AVAILABLE_TABLES:
        return json.dumps({"error": f"Tabla '{table_name}' no permitida."})

    where = f"WHERE {filters}" if filters.strip() else ""
    sql = f"SELECT COUNT(*) FROM dbo.{table_name} WITH (NOLOCK) {where}"

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        total = cursor.fetchone()[0]
    except Exception as e:
        cursor.close()
        conn.close()
        return json.dumps({"error": str(e)})
    finally:
        cursor.close()
        conn.close()

    return json.dumps({"total": total})


@mcp.tool
def run_custom_query(sql: str) -> str:
    """
    Ejecuta una consulta SQL SELECT personalizada sobre las tablas OFIMA.
    Solo se permiten consultas SELECT.

    Args:
        sql: Consulta SQL SELECT, ej: SELECT TOP 10 * FROM dbo.Mvtrade
    """
    sql_clean = sql.strip().upper()
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE"]
    if not sql_clean.startswith("SELECT"):
        return json.dumps({"error": "Solo se permiten consultas SELECT."})
    for word in forbidden:
        if word in sql_clean:
            return json.dumps({"error": f"Operación '{word}' no permitida."})

    conn = get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = [row_to_dict(cursor, row) for row in cursor.fetchall()]
    except Exception as e:
        cursor.close()
        conn.close()
        return json.dumps({"error": str(e)})
    finally:
        cursor.close()
        conn.close()

    return json.dumps(rows, default=serialize, ensure_ascii=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    port = int(os.environ.get("PORT", 8000))
    logging.info(f"Iniciando MCP server OFIMA directo en puerto {port} ...")
    mcp.run(
        transport="http",
        host="0.0.0.0",
        port=port,
    )
