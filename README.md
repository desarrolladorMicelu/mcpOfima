# SyncOFIMA

Replicación automática de tablas OFIMA (SQL Server) → Supabase (PostgreSQL)  
+ MCP Server para consultar los datos desde Claude.

---

## Estructura

```
SyncOFIMA/
├── replicate.py      # Script de replicación
├── mcp_server.py     # MCP server para Claude
├── requirements.txt
├── .env.example      # Plantilla de variables de entorno
└── README.md
```

---

## Setup inicial

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

> En Windows necesitas tener instalado **ODBC Driver 17 for SQL Server**.  
> Descarga: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

### 2. Crear el archivo `.env`

```bash
cp .env.example .env
```

Llena las variables:

```env
SQLSERVER_HOST=172.200.231.95
SQLSERVER_DB=MICELU
SQLSERVER_USER=db_read
SQLSERVER_PASSWORD=tu_password
SQLSERVER_DRIVER=ODBC Driver 17 for SQL Server

DATABASE_URL=postgresql://user:password@host:5432/dbname
```

---

## Replicación

### Ejecutar manualmente

```bash
python replicate.py
```

### Cómo funciona

- Lee cada tabla desde SQL Server (OFIMA)
- Borra y recrea la tabla en Supabase (sobrescritura total)
- Reintentos automáticos: máximo 3, con backoff exponencial (10s, 20s, 40s)
- Logs por stdout (visibles en Railway dashboard)

### Configurar en Railway (Cron Job)

1. En tu proyecto Railway → **New Service** → **Empty Service**
2. En Settings → **Source**: apunta a este repositorio
3. En Settings → **Start Command**: `python replicate.py`
4. En **Variables**: agrega todas las del `.env`
5. En **Cron Schedule**: `0 2 * * *` (todos los días a las 2:00 AM UTC)

> ⚠️ Ajusta la hora al timezone de tu servidor. Si OFIMA está en Colombia (UTC-5), usa `0 7 * * *` para que corra a las 2:00 AM hora Colombia.

---

## MCP Server (Claude.ai web)

### Desplegar en Railway

1. En tu proyecto Railway → **New Service** → apunta al repositorio
2. Railway detecta el `Procfile` y corre `python mcp_server.py` automáticamente
3. Agrega la variable `DATABASE_URL` en las variables del servicio
4. Railway te da una URL pública tipo `https://tu-app.up.railway.app`

### Conectar en Claude.ai web

1. En Claude.ai → icono de herramientas → **Agregar conector personalizado**
2. **Nombre**: OFIMA
3. **URL del servidor MCP remoto**: `https://tu-app.up.railway.app/mcp`
4. Guardar

Verás las herramientas disponibles:

| Herramienta | Descripción |
|---|---|
| `list_tables` | Lista las tablas disponibles |
| `describe_table` | Columnas y tipos de una tabla |
| `query_table` | Consulta con filtros opcionales |
| `count_rows` | Cuenta filas con filtros opcionales |
| `run_custom_query` | SQL SELECT personalizado |

---

## Troubleshooting

### Error: `[ODBC Driver 17] Cannot open server`
- Verifica que el host `172.200.231.95` sea accesible desde donde corres el script
- En Railway puede requerir que el servidor SQL Server tenga acceso desde IPs externas

### Error: `SSL connection required` en Supabase
- Agrega `?sslmode=require` al final del `DATABASE_URL`:  
  `postgresql://user:pass@host:5432/db?sslmode=require`

### Error: `column X is of type Y but expression is of type Z`
- Supabase es estricto con tipos. El script infiere tipos básicos pero puede fallar con tipos especiales de SQL Server.
- Solución: edita el `pg_type_map` en `replicate.py` para ajustar la columna problemática.

### La tabla llega vacía
- Confirma que el usuario `db_read` tiene permisos de SELECT en esa tabla en OFIMA.
- Prueba la consulta directamente: `SELECT TOP 10 * FROM dbo.nombre_tabla`

### MCP no aparece en Claude
- Verifica que la ruta en `args` sea la ruta absoluta correcta
- Corre `python mcp_server.py` manualmente para ver si hay errores
- Revisa los logs de Claude en `%APPDATA%\Claude\logs\`

### Cambiar esquema de OFIMA
Si las tablas no están en `dbo`, agrega al `.env`:
```env
OFIMA_SCHEMA=nombre_esquema
```
