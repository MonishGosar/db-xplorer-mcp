import os
import psycopg2
import threading
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from fastmcp import FastMCP

# ---------------------------------------------------------
# DB CONNECTION POOL
# ---------------------------------------------------------

POOL_LOCK = threading.Lock()
CONNECTION_POOL: ThreadedConnectionPool | None = None


def clean_env(value, default=None):
    if value is None:
        return default
    return value.strip().strip('"').strip("'") or default


def get_port():
    try:
        return int(clean_env(os.environ.get("DB_PORT"), "5432"))
    except:
        return 5432


def init_pool():
    global CONNECTION_POOL
    with POOL_LOCK:
        if CONNECTION_POOL is None:
            CONNECTION_POOL = ThreadedConnectionPool(
                minconn=int(os.environ.get("DB_POOL_MIN", "1")),
                maxconn=int(os.environ.get("DB_POOL_MAX", "5")),
                host=clean_env(os.environ.get("DB_HOST")),
                port=get_port(),
                dbname=clean_env(os.environ.get("DB_NAME")),
                user=clean_env(os.environ.get("DB_USER")),
                password=clean_env(os.environ.get("DB_PASSWORD"))
            )


def get_conn():
    if CONNECTION_POOL is None:
        init_pool()
    conn = CONNECTION_POOL.getconn()
    conn.autocommit = True
    return conn


def release_conn(conn):
    if CONNECTION_POOL and conn:
        CONNECTION_POOL.putconn(conn)


# ---------------------------------------------------------
# MCP SERVER SETUP
# ---------------------------------------------------------

mcp = FastMCP("db-explorer")


# ---------------------------------------------------------
# TOOL: list_schemas
# ---------------------------------------------------------

@mcp.tool()
def list_schemas() -> dict:
    """Return all non-system schemas."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schema_name;
    """)

    schemas = [row[0] for row in cur.fetchall()]
    cur.close()
    release_conn(conn)
    return {"schemas": schemas}


# ---------------------------------------------------------
# TOOL: list_tables(schema)
# ---------------------------------------------------------

@mcp.tool()
def list_tables(schema: str) -> dict:
    """Return all tables inside a given schema."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name;
    """, (schema,))

    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    release_conn(conn)
    return {"schema": schema, "tables": tables}


# ---------------------------------------------------------
# TOOL: describe_table(schema, table)
# ---------------------------------------------------------

@mcp.tool()
def describe_table(schema: str, table: str) -> dict:
    """Return column names + datatypes for a table."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """, (schema, table))

    rows = cur.fetchall()
    cur.close()
    release_conn(conn)

    if not rows:
        return {"error": f"Table '{schema}.{table}' not found"}

    columns = [{"name": r[0], "data_type": r[1]} for r in rows]
    return {"schema": schema, "table": table, "columns": columns}


# ---------------------------------------------------------
# TOOL: preview_rows(schema, table, limit)
# ---------------------------------------------------------

@mcp.tool()
def preview_rows(schema: str, table: str, limit: int = 20) -> dict:
    """Return first N rows of a table."""
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(f"SELECT * FROM {schema}.{table} LIMIT %s;", (limit,))
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        cur.close()
        release_conn(conn)
        return {"schema": schema, "table": table, "columns": cols, "rows": rows}
    except Exception as e:
        cur.close()
        release_conn(conn)
        return {"error": str(e)}


# ---------------------------------------------------------
# TOOL: run_query_safe(sql)
# ---------------------------------------------------------

FORBIDDEN = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]


@mcp.tool()
def run_query_safe(sql: str) -> dict:
    """
    Execute SELECT queries (joins allowed). 
    Blocks modification queries for safety.
    """
    cleaned = sql.lower().strip()

    # Block write operations
    for word in FORBIDDEN:
        if cleaned.startswith(word) or f" {word} " in cleaned:
            return {"error": f"Operation '{word}' is not allowed"}

    if not cleaned.startswith("select"):
        return {"error": "Only SELECT queries are permitted"}

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        cur.close()
        release_conn(conn)
        return {"columns": cols, "rows": rows}

    except Exception as e:
        cur.close()
        release_conn(conn)
        return {"error": str(e)}


# ---------------------------------------------------------
# RUN SERVER
# ---------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
