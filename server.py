import os
import psycopg2
import re
import time
import threading
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
from fastmcp import FastMCP
from difflib import SequenceMatcher


# ---------------------------------------------------------
# DB Connection
# ---------------------------------------------------------

POOL_LOCK = threading.Lock()
CONNECTION_POOL: ThreadedConnectionPool | None = None

CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "300"))
METADATA_CACHE = {
    "schemas": {"data": [], "timestamp": 0},
    "tables": {},  # schema -> {"data": [table_names], "timestamp": ts}
    "table_to_schema": {}  # table_name -> set([schemas])
}
CACHE_LOCK = threading.Lock()

CONTEXT_STATE = {
    "last_schema": None,
    "last_table": None
}


def clean_env_var(value, default=None):
    """Strip quotes and whitespace from environment variable values"""
    if value is None:
        return default
    # Remove surrounding quotes (single or double) and strip whitespace
    value = value.strip().strip('"').strip("'")
    return value if value else default


def get_port_value():
    port = clean_env_var(os.environ.get("DB_PORT"), "5432")
    try:
        return int(port)
    except (ValueError, TypeError):
        return 5432


def init_connection_pool():
    global CONNECTION_POOL
    with POOL_LOCK:
        if CONNECTION_POOL is None:
            min_conn = int(os.environ.get("DB_POOL_MIN", "1"))
            max_conn = int(os.environ.get("DB_POOL_MAX", "5"))
            CONNECTION_POOL = ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                host=clean_env_var(os.environ.get("DB_HOST")),
                port=get_port_value(),
                dbname=clean_env_var(os.environ.get("DB_NAME")),
                user=clean_env_var(os.environ.get("DB_USER")),
                password=clean_env_var(os.environ.get("DB_PASSWORD"))
            )


def release_conn(conn):
    if conn is None:
        return
    raw = getattr(conn, "_raw", None)
    if raw is None:
        try:
            conn.close()
        except Exception:
            pass
        return
    if CONNECTION_POOL:
        CONNECTION_POOL.putconn(raw)


class PooledConnection:
    def __init__(self, raw_conn):
        self._raw = raw_conn

    def __getattr__(self, item):
        return getattr(self._raw, item)

    def close(self):
        release_conn(self)
        self._raw = None


def get_conn():
    if CONNECTION_POOL is None:
        init_connection_pool()
    raw = CONNECTION_POOL.getconn()
    raw.autocommit = True
    return PooledConnection(raw)


# ---------------------------------------------------------
# Metadata caching helpers
# ---------------------------------------------------------


def _is_cache_valid(entry):
    if not entry or not entry.get("data"):
        return False
    return (time.time() - entry.get("timestamp", 0)) < CACHE_TTL_SECONDS


def _update_table_to_schema_map(schema, table_names):
    with CACHE_LOCK:
        for name in table_names:
            if name not in METADATA_CACHE["table_to_schema"]:
                METADATA_CACHE["table_to_schema"][name] = set()
            METADATA_CACHE["table_to_schema"][name].add(schema)


def get_cached_schemas(conn=None, force_refresh=False):
    with CACHE_LOCK:
        entry = METADATA_CACHE["schemas"]
        if not force_refresh and _is_cache_valid(entry):
            return entry["data"]

    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True

    cur = conn.cursor()
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name LIKE 'collections_%'
           OR schema_name LIKE 'recovery_%'
           OR schema_name = 'gold'
        ORDER BY schema_name;
    """)
    schemas = [row[0] for row in cur.fetchall()]
    cur.close()

    if close_conn:
        conn.close()

    with CACHE_LOCK:
        METADATA_CACHE["schemas"] = {"data": schemas, "timestamp": time.time()}

    return schemas


def get_cached_table_names(schema, conn=None, force_refresh=False):
    with CACHE_LOCK:
        entry = METADATA_CACHE["tables"].get(schema)
        if entry and not force_refresh and _is_cache_valid(entry):
            return entry["data"]

    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True

    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """, (schema,))
    names = [row[0] for row in cur.fetchall()]
    cur.close()

    if close_conn:
        conn.close()

    with CACHE_LOCK:
        METADATA_CACHE["tables"][schema] = {"data": names, "timestamp": time.time()}
    _update_table_to_schema_map(schema, names)

    return names


def update_table_cache_from_results(schema, table_names):
    if not table_names:
        return
    with CACHE_LOCK:
        entry = METADATA_CACHE["tables"].get(schema, {"data": [], "timestamp": 0})
        existing = set(entry.get("data", []))
        existing.update(table_names)
        METADATA_CACHE["tables"][schema] = {"data": list(existing), "timestamp": time.time()}
    _update_table_to_schema_map(schema, table_names)


def refresh_table_schema_map(conn=None):
    schemas = get_cached_schemas(conn=conn)
    for schema in schemas:
        get_cached_table_names(schema, conn=conn, force_refresh=True)


def update_context(schema=None, table=None):
    if schema:
        CONTEXT_STATE["last_schema"] = schema
    if table:
        CONTEXT_STATE["last_table"] = table


def find_table_across_schemas(table, conn=None):
    with CACHE_LOCK:
        schema_set = METADATA_CACHE["table_to_schema"].get(table)
        if schema_set:
            return [{"schema": s, "table": table} for s in schema_set]

    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True

    cur = conn.cursor()
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name = %s
          AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY table_schema;
    """, (table,))
    rows = cur.fetchall()
    cur.close()

    if close_conn:
        conn.close()

    for schema_name, table_name in rows:
        update_table_cache_from_results(schema_name, [table_name])

    return [{"schema": row[0], "table": row[1]} for row in rows]


mcp = FastMCP("db-xplorer")


# ---------------------------------------------------------
# Helper: Check if table exists and get suggestions
# ---------------------------------------------------------

def check_table_exists(schema: str, table: str, conn=None):
    """Check if a table exists and return suggestions if it doesn't"""
    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True
    
    cur = conn.cursor()
    
    try:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema, table))
        
        exists = cur.fetchone()[0]
        
        if exists:
            cur.close()
            if close_conn:
                conn.close()
            return {"exists": True, "suggestions": []}
        
        # Table doesn't exist - find similar tables
        suggestions = []
        
        # Find tables with similar names in the same schema
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name ILIKE %s
            ORDER BY table_name
            LIMIT 10;
        """, (schema, f"%{table}%"))
        
        similar = [row[0] for row in cur.fetchall()]
        suggestions.extend(similar)
        
        # Find tables with similar names in other schemas
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND table_name ILIKE %s
            ORDER BY table_schema, table_name
            LIMIT 10;
        """, (f"%{table}%",))
        
        for row in cur.fetchall():
            if row[0] != schema or row[1] != table:
                suggestions.append(f"{row[0]}.{row[1]}")
        
        # Get all tables in the schema as fallback
        if not suggestions:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT 20;
            """, (schema,))
            
            all_tables = [row[0] for row in cur.fetchall()]
            suggestions.extend(all_tables)
        
        cur.close()
        if close_conn:
            conn.close()
        
        return {
            "exists": False,
            "suggestions": list(set(suggestions))[:15],  # Remove duplicates, limit to 15
            "message": f"Table '{schema}.{table}' does not exist"
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        if close_conn:
            conn.close()
        return {"exists": False, "suggestions": [], "error": str(e)}


# ---------------------------------------------------------
# Tool: list_schemas
# ---------------------------------------------------------

@mcp.tool()
def list_schemas() -> dict:
    schemas = get_cached_schemas()
    return {"schemas": schemas}


# ---------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------

@mcp.tool()
def list_tables(schema: str) -> dict:
    conn = get_conn()
    cur = conn.cursor()

    # table descriptions (metadata)
    try:
        cur.execute("""
            SELECT table_name, description
            FROM metadata.table_description
            WHERE schema_name = %s;
        """, (schema,))
        desc_map = {r[0]: r[1] for r in cur.fetchall()}
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
        desc_map = {}

    # row estimates
    cur.execute("""
        SELECT relname AS table_name,
               reltuples::bigint AS row_estimate
        FROM pg_class
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE nspname = %s AND relkind='r';
    """, (schema,))

    result = []
    table_names_for_cache = []
    for table_name, estimate in cur.fetchall():
        table_names_for_cache.append(table_name)
        result.append({
            "table_name": table_name,
            "row_estimate": int(estimate),
            "description": desc_map.get(table_name, "")
        })

    cur.close()
    conn.close()
    update_table_cache_from_results(schema, table_names_for_cache)
    return {"tables": result}


# ---------------------------------------------------------
# Tool: describe_table
# ---------------------------------------------------------

@mcp.tool()
def describe_table(schema: str, table: str) -> dict:
    conn = get_conn()
    
    # Check if table exists first
    check_result = check_table_exists(schema, table, conn)
    if not check_result.get("exists", False):
        conn.close()
        return {
            "schema": schema,
            "table": table,
            "error": check_result.get("message", f"Table '{schema}.{table}' does not exist"),
            "suggestions": check_result.get("suggestions", [])[:10],
            "found_in_other_schemas": check_result.get("similar_tables", []),
            "hint": f"Did you mean one of these? {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else f"Use 'list_tables' to see available tables in schema '{schema}' or 'find_table_schema' to search for '{table}'"
        }
    
    cur = conn.cursor()

    # meta info
    try:
        cur.execute("""
            SELECT description, grain
            FROM metadata.table_description
            WHERE schema_name=%s AND table_name=%s;
        """, (schema, table))

        row = cur.fetchone()
        description = row[0] if row else ""
        grain = row[1] if row else ""
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
        description = ""
        grain = ""

    # all columns
    try:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position;
        """, (schema, table))

        columns = cur.fetchall()
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {
            "schema": schema,
            "table": table,
            "description": "",
            "grain": "",
            "dimensions": [],
            "measures": [],
            "sample_columns": [],
            "error": "Failed to fetch table information"
        }

    # dimensions/measures metadata
    try:
        cur.execute("""
            SELECT column_name, description, role
            FROM metadata.column_description
            WHERE schema_name=%s AND table_name=%s;
        """, (schema, table))

        meta = cur.fetchall()
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
        meta = []

    dim_list, meas_list = [], []
    for col_name, col_desc, role in meta:
        dtype = next((c[1] for c in columns if c[0] == col_name), None)
        obj = {
            "name": col_name,
            "data_type": dtype,
            "description": col_desc
        }
        if role == "dimension":
            dim_list.append(obj)
        else:
            meas_list.append(obj)

    # sample stats on 5 columns
    sample_cols = columns[:5]
    sample_out = []

    for col_name, dtype in sample_cols:
        try:
            cur.execute(
                f"SELECT MIN({col_name}), MAX({col_name}), COUNT(DISTINCT {col_name}) "
                f"FROM {schema}.{table};"
            )
            mn, mx, distinct = cur.fetchone()
        except:
            mn = mx = distinct = None

        sample_out.append({
            "column": col_name,
            "data_type": dtype,
            "min": mn,
            "max": mx,
            "distinct_count": distinct
        })

    cur.close()
    conn.close()

    update_context(schema, table)

    return {
        "schema": schema,
        "table": table,
        "description": description,
        "grain": grain,
        "dimensions": dim_list,
        "measures": meas_list,
        "sample_columns": sample_out
    }


# ---------------------------------------------------------
# Tool: search_columns
# ---------------------------------------------------------

@mcp.tool()
def search_columns(pattern: str) -> dict:
    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT schema_name, table_name, column_name, data_type, description
            FROM metadata.column_description
            WHERE column_name ILIKE %s OR description ILIKE %s
            ORDER BY schema_name, table_name;
        """, (f"%{pattern}%", f"%{pattern}%"))
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {"columns": []}

    out = []
    for r in cur.fetchall():
        out.append({
            "schema": r[0],
            "table": r[1],
            "column": r[2],
            "data_type": r[3],
            "description": r[4]
        })

    cur.close()
    conn.close()
    return {"columns": out}


# ---------------------------------------------------------
# Helper: Check if table exists and get suggestions
# ---------------------------------------------------------

def check_table_exists(schema: str, table: str, conn=None):
    """
    Check if a table exists in the given schema.
    Returns suggestions for similar table names if not found.
    Also searches other schemas if table not found in specified schema.
    """
    result = {
        "exists": False,
        "schema": schema,
        "table": table,
        "actual_schema": None,
        "suggestions": [],
        "similar_tables": []
    }

    try:
        table_names = get_cached_table_names(schema, conn=conn)
    except Exception as e:
        result["message"] = f"Error checking table: {str(e)}"
        return result

    if table in table_names:
        result["exists"] = True
        result["actual_schema"] = schema
        return result

    # Similar names within same schema
    similar_in_schema = [name for name in table_names if table.lower() in name.lower()]
    if not similar_in_schema:
        # fallback to fuzzy match using SequenceMatcher
        similar_in_schema = sorted(
            table_names,
            key=lambda name: similarity(name, table),
            reverse=True
        )[:5]

    result["suggestions"].extend([f"{schema}.{name}" for name in similar_in_schema])

    # Find table in other schemas using cache/DB
    found_elsewhere = find_table_across_schemas(table, conn=conn)
    if found_elsewhere:
        result["similar_tables"] = found_elsewhere
        result["suggestions"].extend([f"{entry['schema']}.{entry['table']}" for entry in found_elsewhere])
        result["message"] = (
            f"Table '{table}' not found in schema '{schema}', but exists in: "
            f"{', '.join({entry['schema'] for entry in found_elsewhere})}"
        )
    elif result["suggestions"]:
        result["message"] = (
            f"Table '{table}' not found in schema '{schema}'. "
            f"Similar tables: {', '.join(result['suggestions'][:5])}"
        )
    else:
        result["message"] = (
            f"Table '{schema}.{table}' does not exist. "
            f"Use 'list_tables' to inspect schema '{schema}' or 'find_table_schema' to locate the table."
        )

    return result


# ---------------------------------------------------------
# Tool: verify_table_exists
# ---------------------------------------------------------

@mcp.tool()
def verify_table_exists(schema: str, table: str) -> dict:
    """
    Check if a table exists in the specified schema.
    If not found, provides suggestions for similar table names and searches other schemas.
    Use this before querying tables to avoid errors.
    """
    result = check_table_exists(schema, table)
    
    return {
        "exists": result["exists"],
        "schema": schema,
        "table": table,
        "actual_schema": result.get("actual_schema"),
        "message": result.get("message", ""),
        "suggestions": result.get("suggestions", [])[:10],
        "found_in_other_schemas": result.get("similar_tables", [])
    }


# ---------------------------------------------------------
# Tool: find_table_schema
# ---------------------------------------------------------

@mcp.tool()
def find_table_schema(table_name: str) -> dict:
    """
    Find which schema(s) contain a table with the given name.
    Useful when you know the table name but not which schema it's in.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Exact match
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name = %s
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY table_schema;
        """, (table_name,))
        
        exact_matches = [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]
        
        # Similar matches (fuzzy)
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name ILIKE %s
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND table_name != %s
            ORDER BY table_schema, table_name
            LIMIT 10;
        """, (f"%{table_name}%", table_name))
        
        similar_matches = [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return {
            "table_name": table_name,
            "exact_matches": exact_matches,
            "similar_matches": similar_matches,
            "found": len(exact_matches) > 0,
            "message": f"Found in {len(exact_matches)} schema(s)" if exact_matches else f"Table '{table_name}' not found. Similar: {', '.join([m['schema'] + '.' + m['table'] for m in similar_matches[:5]])}"
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {"error": str(e)}


# ---------------------------------------------------------
# Tool: preview_rows
# ---------------------------------------------------------

@mcp.tool()
def preview_rows(schema: str, table: str, limit: int = 20) -> dict:
    """
    Preview first N rows of a table. Automatically verifies table exists first.
    """
    conn = get_conn()
    
    # Check if table exists first
    check_result = check_table_exists(schema, table, conn)
    if not check_result.get("exists", False):
        conn.close()
        return {
            "error": check_result.get("message", f"Table '{schema}.{table}' does not exist"),
            "suggestions": check_result.get("suggestions", [])[:10],
            "found_in_other_schemas": check_result.get("similar_tables", []),
            "hint": f"Did you mean one of these? {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else f"Use 'list_tables' to see available tables in schema '{schema}' or 'find_table_schema' to search for '{table}'"
        }
    
    cur = conn.cursor()

    try:
        cur.execute(
            f"SELECT * FROM {schema}.{table} LIMIT %s;",
            (limit,)
        )

        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()

        cur.close()
        conn.close()

        update_context(schema, table)

        return {
            "schema": schema,
            "table": table,
            "columns": cols,
            "rows": rows,
            "row_count": len(rows)
        }
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        
        # Try to get suggestions even on error
        check_result = check_table_exists(schema, table)
        return {
            "error": str(e),
            "suggestions": check_result.get("suggestions", [])[:10],
            "found_in_other_schemas": check_result.get("similar_tables", []),
            "hint": f"Table may not exist. Try: {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else "Use 'list_tables' to see available tables or 'find_table_schema' to search"
        }


# ---------------------------------------------------------
# Tool: get_row_count
# ---------------------------------------------------------

@mcp.tool()
def get_row_count(schema: str, table: str) -> dict:
    """
    Get approximate or exact row count for a table. Automatically verifies table exists first.
    """
    conn = get_conn()
    
    # Check if table exists first
    check_result = check_table_exists(schema, table, conn)
    if not check_result.get("exists", False):
        conn.close()
        return {
            "error": check_result.get("message", f"Table '{schema}.{table}' does not exist"),
            "suggestions": check_result.get("suggestions", [])[:10],
            "found_in_other_schemas": check_result.get("similar_tables", []),
            "hint": f"Did you mean one of these? {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else f"Use 'list_tables' to see available tables in schema '{schema}' or 'find_table_schema' to search for '{table}'"
        }
    
    cur = conn.cursor()

    try:
        # estimate
        cur.execute("""
            SELECT reltuples::bigint
            FROM pg_class
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE nspname=%s AND relname=%s;
        """, (schema, table))
        estimate = cur.fetchone()[0]

        # exact count
        try:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            exact = cur.fetchone()[0]
        except Exception:
            # Rollback if transaction was aborted
            try:
                conn.rollback()
            except Exception:
                pass
            exact = None

        cur.close()
        conn.close()

        update_context(schema, table)

        return {"row_estimate": estimate, "row_exact": exact}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        
        check_result = check_table_exists(schema, table)
        return {
            "error": str(e),
            "suggestions": check_result.get("suggestions", [])[:10],
            "found_in_other_schemas": check_result.get("similar_tables", []),
            "hint": f"Table may not exist. Try: {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else "Use 'list_tables' to see available tables or 'find_table_schema' to search"
        }


# ---------------------------------------------------------
# Tool: run_query_safe
# ---------------------------------------------------------

FORBIDDEN = ["update", "delete", "insert", "alter", "drop", "truncate", "create"]

BLOCK_PATTERNS = [
    r"cross join",
    r",\s*\w+\s*"
]


@mcp.tool()
def run_query_safe(sql: str) -> dict:
    cleaned = sql.lower().strip()

    # only SELECT allowed
    if not cleaned.startswith("select"):
        return {"error": "Only SELECT queries allowed"}

    # block harmful keywords
    for bad in FORBIDDEN:
        if bad in cleaned:
            return {"error": f"Operation '{bad}' is not allowed"}

    # block cross joins
    for pat in BLOCK_PATTERNS:
        if re.search(pat, cleaned):
            return {"error": "Query blocked for safety"}

    # enforce LIMIT
    if "limit" not in cleaned:
        sql = sql.rstrip(";") + " LIMIT 200;"

    conn = get_conn()
    cur = conn.cursor()

    try:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    except Exception as e:
        error_msg = str(e)
        cur.close()
        conn.close()
        
        # Try to extract table name from error and suggest alternatives
        suggestions = []
        if "does not exist" in error_msg or "relation" in error_msg:
            # Try to extract schema.table from error
            match = re.search(r'relation\s+"?([^."]+)\.([^."]+)"?', error_msg, re.IGNORECASE)
            if match:
                schema = match.group(1)
                table = match.group(2)
                check_result = check_table_exists(schema, table)
                suggestions = check_result.get("suggestions", [])
        
        result = {"error": error_msg}
        if suggestions:
            result["suggestions"] = suggestions
            result["hint"] = f"Table may not exist. Did you mean: {', '.join(suggestions[:5])}"
        else:
            result["hint"] = "Check the table name and schema. Use 'list_tables' to see available tables."
        
        return result

    cur.close()
    conn.close()

    return {"columns": cols, "rows": rows}


# ---------------------------------------------------------
# Tool: smart_search - Comprehensive search across everything
# ---------------------------------------------------------

@mcp.tool()
def smart_search(query: str, max_results: int = 50) -> dict:
    """
    Comprehensive search across schemas, tables, columns, and data.
    Searches for the query term in schema names, table names, column names,
    column descriptions, and actual data values.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    query_lower = query.lower()
    query_pattern = f"%{query_lower}%"
    
    results = {
        "schemas": [],
        "tables": [],
        "columns": [],
        "data_matches": []
    }
    
    try:
        # Search schemas
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ILIKE %s
            ORDER BY schema_name
            LIMIT 20;
        """, (query_pattern,))
        results["schemas"] = [row[0] for row in cur.fetchall()]
        
        # Search tables across all schemas
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND (table_schema ILIKE %s OR table_name ILIKE %s)
            ORDER BY table_schema, table_name
            LIMIT 30;
        """, (query_pattern, query_pattern))
        results["tables"] = [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]
        
        # Search columns
        cur.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND column_name ILIKE %s
            ORDER BY table_schema, table_name, column_name
            LIMIT 50;
        """, (query_pattern,))
        results["columns"] = [
            {
                "schema": row[0],
                "table": row[1],
                "column": row[2],
                "data_type": row[3]
            }
            for row in cur.fetchall()
        ]
        
        # Search in metadata descriptions if available
        try:
            cur.execute("""
                SELECT schema_name, table_name, column_name, description
                FROM metadata.column_description
                WHERE description ILIKE %s
                ORDER BY schema_name, table_name
                LIMIT 30;
            """, (query_pattern,))
            for row in cur.fetchall():
                results["columns"].append({
                    "schema": row[0],
                    "table": row[1],
                    "column": row[2],
                    "description": row[3],
                    "matched_in": "description"
                })
        except Exception:
            pass
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": str(e), "results": results}
    
    cur.close()
    conn.close()
    
    return {
        "query": query,
        "total_matches": {
            "schemas": len(results["schemas"]),
            "tables": len(results["tables"]),
            "columns": len(results["columns"])
        },
        "results": results
    }


# ---------------------------------------------------------
# Tool: deep_search - Search inside table data
# ---------------------------------------------------------

@mcp.tool()
def deep_search(schema: str, table: str, search_term: str, limit: int = 100) -> dict:
    """
    Search for a term inside actual table data.
    Searches across all text/varchar columns in the specified table.
    """
    conn = get_conn()
    
    # Check if table exists first
    check_result = check_table_exists(schema, table, conn)
    if not check_result.get("exists", False):
        conn.close()
        return {
            "error": check_result.get("message", f"Table '{schema}.{table}' does not exist"),
            "suggestions": check_result.get("suggestions", []),
            "hint": f"Did you mean one of these? {', '.join(check_result.get('suggestions', [])[:5])}" if check_result.get("suggestions") else f"Use 'list_tables' to see available tables in schema '{schema}'"
        }
    
    cur = conn.cursor()
    
    try:
        # Get all text/varchar columns
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            AND data_type IN ('text', 'varchar', 'character varying', 'char')
            ORDER BY ordinal_position;
        """, (schema, table))
        
        text_columns = cur.fetchall()
        
        if not text_columns:
            cur.close()
            conn.close()
            return {
                "schema": schema,
                "table": table,
                "search_term": search_term,
                "message": "No text/varchar columns found in this table",
                "matches": []
            }
        
        # Build search query across all text columns
        search_pattern = f"%{search_term}%"
        column_names = [col[0] for col in text_columns]
        
        # Create OR conditions for all text columns
        conditions = " OR ".join([f"{col}::text ILIKE %s" for col in column_names])
        
        query = f"""
            SELECT *
            FROM {schema}.{table}
            WHERE {conditions}
            LIMIT %s;
        """
        
        params = tuple([search_pattern] * len(column_names) + [limit])
        
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        
        matches = []
        for row in rows:
            match_info = {}
            for idx, col_name in enumerate(cols):
                if col_name in column_names and row[idx] and search_term.lower() in str(row[idx]).lower():
                    match_info[col_name] = str(row[idx])
            if match_info:
                matches.append({
                    "row_data": dict(zip(cols, row)),
                    "matched_columns": match_info
                })
        
        cur.close()
        conn.close()
        
        update_context(schema, table)

        return {
            "schema": schema,
            "table": table,
            "search_term": search_term,
            "columns_searched": column_names,
            "total_matches": len(matches),
            "matches": matches[:limit]
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {"error": str(e)}


# ---------------------------------------------------------
# Helper: Fuzzy similarity matching
# ---------------------------------------------------------

def similarity(a, b):
    """Calculate similarity ratio between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def extract_keywords_flexible(query: str):
    """
    Extract keywords from query with flexible matching.
    Handles typos, variations, and partial matches.
    """
    # Common stop words to ignore
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
                  'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'what', 'where',
                  'when', 'who', 'how', 'which', 'that', 'this', 'these', 'those'}
    
    query_lower = query.lower()
    
    # Extract all words, filtering stop words and very short words
    words = [w.strip('.,!?;:()[]{}') for w in query_lower.split()]
    keywords = [w for w in words if len(w) > 2 and w not in stop_words]
    
    # Also try to extract phrases (2-3 word combinations)
    phrases = []
    for i in range(len(keywords) - 1):
        phrases.append(f"{keywords[i]} {keywords[i+1]}")
    for i in range(len(keywords) - 2):
        phrases.append(f"{keywords[i]} {keywords[i+1]} {keywords[i+2]}")
    
    return keywords, phrases

def generate_search_patterns(term: str):
    """
    Generate multiple search patterns for fuzzy matching.
    Handles typos and variations.
    """
    patterns = []
    term_lower = term.lower()
    
    # Exact match
    patterns.append(f"%{term_lower}%")
    
    # Partial matches (first 3+ chars)
    if len(term_lower) > 3:
        patterns.append(f"%{term_lower[:3]}%")
        patterns.append(f"%{term_lower[:4]}%")
    
    # Common variations/typos
    variations = {
        'prediction': ['prediction', 'predict', 'predicted', 'predictions'],
        'precision': ['precision', 'precise', 'precisely'],
        'accuracy': ['accuracy', 'accurate', 'accurately'],
        'november': ['nov', 'november', '11'],
        'december': ['dec', 'december', '12'],
    }
    
    for key, variants in variations.items():
        if key in term_lower or any(v in term_lower for v in variants):
            patterns.extend([f"%{v}%" for v in variants])
    
    return list(set(patterns))  # Remove duplicates


# ---------------------------------------------------------
# Tool: analyze_data - Natural language query analysis (ROBUST VERSION)
# ---------------------------------------------------------

@mcp.tool()
def analyze_data(query: str, date_filter: str = None) -> dict:
    """
    Analyze data based on ANY natural language query - handles typos, variations, and flexible matching.
    Example: "what is precision for nov 23" or "show me prediction data november" - works with any query.
    Automatically searches schemas, tables, columns with multiple strategies and fallbacks.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    query_lower = query.lower()
    
    # Extract keywords flexibly
    keywords, phrases = extract_keywords_flexible(query)
    if not keywords:
        if CONTEXT_STATE.get("last_table"):
            keywords.append(CONTEXT_STATE["last_table"])
        elif CONTEXT_STATE.get("last_schema"):
            keywords.append(CONTEXT_STATE["last_schema"])
    
    # Extract date information (more flexible)
    date_patterns = {
        "nov": "11", "november": "11", "nov.": "11",
        "dec": "12", "december": "12", "dec.": "12",
        "jan": "01", "january": "01", "jan.": "01",
        "feb": "02", "february": "02", "feb.": "02",
        "mar": "03", "march": "03", "mar.": "03",
        "apr": "04", "april": "04", "apr.": "04",
        "may": "05",
        "jun": "06", "june": "06", "jun.": "06",
        "jul": "07", "july": "07", "jul.": "07",
        "aug": "08", "august": "08", "aug.": "08",
        "sep": "09", "september": "09", "sep.": "09",
        "oct": "10", "october": "10", "oct.": "10"
    }
    
    month = None
    year = None
    for key, value in date_patterns.items():
        if key in query_lower:
            month = value
            break
    
    # Try to extract year (23, 2023, etc.) - more flexible
    year_patterns = [
        r'\b(20\d{2})\b',  # 2023, 2024, etc.
        r'\b(\d{2})\b',    # 23, 24, etc.
        r'\b(\d{4})\b'     # Any 4-digit year
    ]
    for pattern in year_patterns:
        year_match = re.search(pattern, query_lower)
        if year_match:
            year_str = year_match.group(1)
            if len(year_str) == 2:
                year = f"20{year_str}"
            else:
                year = year_str
            break
    
    results = {
        "query": query,
        "extracted_keywords": keywords,
        "extracted_phrases": phrases,
        "date_info": {
            "month": month,
            "year": year,
            "date_filter": date_filter
        },
        "found_tables": [],
        "found_columns": [],
        "sample_data": [],
        "search_strategies_used": []
    }
    
    try:
        # Strategy 1: Try exact/partial keyword matching
        if keywords:
            search_terms = keywords[:5]  # Use top 5 keywords
            for term in search_terms:
                patterns = generate_search_patterns(term)
                for pattern in patterns[:3]:  # Limit patterns per term
                    try:
                        # Search tables
                        cur.execute("""
                            SELECT table_schema, table_name
                            FROM information_schema.tables
                            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                            AND (table_schema ILIKE %s OR table_name ILIKE %s)
                            ORDER BY table_schema, table_name
                            LIMIT 20;
                        """, (pattern, pattern))
                        
                        for row in cur.fetchall():
                            table_info = {"schema": row[0], "table": row[1]}
                            if table_info not in results["found_tables"]:
                                results["found_tables"].append(table_info)
                        
                        # Search columns
                        cur.execute("""
                            SELECT table_schema, table_name, column_name, data_type
                            FROM information_schema.columns
                            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                            AND column_name ILIKE %s
                            ORDER BY table_schema, table_name, column_name
                            LIMIT 30;
                        """, (pattern,))
                        
                        for row in cur.fetchall():
                            col_info = {
                                "schema": row[0],
                                "table": row[1],
                                "column": row[2],
                                "data_type": row[3]
                            }
                            if col_info not in results["found_columns"]:
                                results["found_columns"].append(col_info)
                    except Exception:
                        continue
            
            results["search_strategies_used"].append("keyword_pattern_matching")
        
        # Strategy 2: Try phrase matching if keywords didn't find much
        if len(results["found_tables"]) < 5 and phrases:
            for phrase in phrases[:3]:
                try:
                    pattern = f"%{phrase}%"
                    cur.execute("""
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        AND (table_schema ILIKE %s OR table_name ILIKE %s)
                        ORDER BY table_schema, table_name
                        LIMIT 10;
                    """, (pattern, pattern))
                    
                    for row in cur.fetchall():
                        table_info = {"schema": row[0], "table": row[1]}
                        if table_info not in results["found_tables"]:
                            results["found_tables"].append(table_info)
                except Exception:
                    continue
            
            results["search_strategies_used"].append("phrase_matching")
        
        # Strategy 3: If still not enough, search ALL interesting schemas and list their tables
        if len(results["found_tables"]) < 3:
            try:
                cur.execute("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name LIKE 'collections_%'
                       OR schema_name LIKE 'recovery_%'
                       OR schema_name = 'gold'
                    ORDER BY schema_name
                    LIMIT 10;
                """)
                
                schemas = [row[0] for row in cur.fetchall()]
                for schema in schemas:
                    cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                        LIMIT 10;
                    """, (schema,))
                    
                    for row in cur.fetchall():
                        table_info = {"schema": schema, "table": row[0]}
                        if table_info not in results["found_tables"]:
                            results["found_tables"].append(table_info)
                            if len(results["found_tables"]) >= 10:
                                break
                    if len(results["found_tables"]) >= 10:
                        break
                
                results["search_strategies_used"].append("schema_exploration_fallback")
            except Exception:
                pass
        
        # Try to find date columns (more flexible matching)
        date_columns = []
        date_keywords = ["date", "time", "created", "updated", "month", "year", "day", "timestamp", "dt"]
        for col_info in results["found_columns"]:
            col_name = col_info.get("column", "").lower()
            if any(term in col_name for term in date_keywords):
                date_columns.append(col_info)
        
        # Also search for date columns in all found tables
        for table_info in results["found_tables"][:5]:
            try:
                schema = table_info["schema"]
                table = table_info["table"]
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    AND (column_name ILIKE %s OR column_name ILIKE %s OR column_name ILIKE %s
                         OR data_type IN ('date', 'timestamp', 'timestamp without time zone', 'timestamp with time zone'))
                    ORDER BY ordinal_position
                    LIMIT 10;
                """, (schema, table, "%date%", "%time%", "%month%"))
                
                for row in cur.fetchall():
                    col_info = {
                        "schema": schema,
                        "table": table,
                        "column": row[0],
                        "data_type": row[1]
                    }
                    if col_info not in date_columns:
                        date_columns.append(col_info)
            except Exception:
                continue
        
        results["date_columns_found"] = date_columns
        
        # If we found relevant tables, try to query them and get sample data
        if results["found_tables"]:
            # Take the first few relevant tables
            for table_info in results["found_tables"][:5]:
                schema = table_info["schema"]
                table = table_info["table"]
                
                try:
                    # Get all columns
                    cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        LIMIT 30;
                    """, (schema, table))
                    
                    columns = cur.fetchall()
                    col_names = [col[0] for col in columns]
                    
                    if col_names:
                        # Try to get sample data
                        try:
                            sample_query = f"SELECT * FROM {schema}.{table} LIMIT 10;"
                            cur.execute(sample_query)
                            sample_cols = [d[0] for d in cur.description]
                            sample_rows = cur.fetchall()
                            
                            results["sample_data"].append({
                                "schema": schema,
                                "table": table,
                                "columns": col_names,
                                "sample_rows": [
                                    dict(zip(sample_cols, row))
                                    for row in sample_rows[:5]
                                ]
                            })
                        except Exception:
                            # If we can't get sample data, at least return column info
                            results["sample_data"].append({
                                "schema": schema,
                                "table": table,
                                "columns": col_names,
                                "sample_rows": [],
                                "note": "Could not fetch sample data"
                            })
                except Exception:
                    continue
        
        cur.close()
        conn.close()
        
        # Generate helpful recommendations
        recommendations = []
        if results["found_tables"]:
            recommendations.append(f"Found {len(results['found_tables'])} relevant tables")
        else:
            recommendations.append("No exact matches found - try exploring schemas with 'list_schemas'")
        
        if results["found_columns"]:
            recommendations.append(f"Found {len(results['found_columns'])} relevant columns")
        
        if date_columns:
            recommendations.append(f"Found {len(date_columns)} date/time columns for filtering")
        
        if month or year:
            recommendations.append(f"Date filter: {month or 'any'}/{year or 'any'}")
        
        recommendations.append("Use 'preview_rows' to see actual data")
        recommendations.append("Use 'deep_search' to search inside specific tables")
        recommendations.append("Use 'run_query_safe' to run custom queries")
        
        return {
            "analysis": results,
            "recommendations": recommendations,
            "success": len(results["found_tables"]) > 0 or len(results["found_columns"]) > 0
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {
            "error": str(e),
            "partial_results": results,
            "message": "Search encountered an error but partial results may be available"
        }


# ---------------------------------------------------------
# Tool: find_data_by_value - Find tables/rows containing specific values
# ---------------------------------------------------------

@mcp.tool()
def find_data_by_value(search_value: str, data_type: str = "text") -> dict:
    """
    Find which tables and rows contain a specific value.
    Searches across all tables in interesting schemas.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    results = {
        "search_value": search_value,
        "matches": []
    }
    
    try:
        # Get all interesting schemas
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name LIKE 'collections_%'
               OR schema_name LIKE 'recovery_%'
               OR schema_name = 'gold'
            ORDER BY schema_name;
        """)
        
        schemas = [row[0] for row in cur.fetchall()]
        
        for schema in schemas[:5]:  # Limit to first 5 schemas for performance
            try:
                # Get all tables in schema
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """, (schema,))
                
                tables = [row[0] for row in cur.fetchall()]
                
                for table in tables[:10]:  # Limit tables per schema
                    try:
                        # Get text columns
                        cur.execute("""
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = %s
                            AND table_name = %s
                            AND data_type IN ('text', 'varchar', 'character varying', 'char')
                            LIMIT 5;
                        """, (schema, table))
                        
                        text_cols = [row[0] for row in cur.fetchall()]
                        
                        if text_cols:
                            # Search in these columns
                            conditions = " OR ".join([f"{col}::text ILIKE %s" for col in text_cols])
                            query = f"""
                                SELECT COUNT(*) as match_count
                                FROM {schema}.{table}
                                WHERE {conditions}
                                LIMIT 1;
                            """
                            
                            cur.execute(query, tuple([f"%{search_value}%"] * len(text_cols)))
                            count = cur.fetchone()[0]
                            
                            if count > 0:
                                results["matches"].append({
                                    "schema": schema,
                                    "table": table,
                                    "match_count": count,
                                    "searched_columns": text_cols
                                })
                    except Exception:
                        continue
            except Exception:
                continue
        
        cur.close()
        conn.close()
        
        return {
            "search_value": search_value,
            "total_matches": len(results["matches"]),
            "matches": results["matches"][:20]  # Limit results
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {"error": str(e)}


# ---------------------------------------------------------
# Tool: verify_table_exists - Check table and get suggestions
# ---------------------------------------------------------

@mcp.tool()
def verify_table_exists(schema: str, table: str) -> dict:
    """
    Check if a table exists and get suggestions for similar table names if it doesn't.
    Use this before querying tables to avoid errors.
    """
    result = check_table_exists(schema, table)
    
    if result.get("exists", False):
        return {
            "exists": True,
            "schema": schema,
            "table": table,
            "message": f"Table '{schema}.{table}' exists and is ready to query"
        }
    else:
        return {
            "exists": False,
            "schema": schema,
            "table": table,
            "error": result.get("message", f"Table '{schema}.{table}' does not exist"),
            "suggestions": result.get("suggestions", []),
            "hint": f"Did you mean one of these? {', '.join(result.get('suggestions', [])[:10])}" if result.get("suggestions") else f"Use 'list_tables' with schema '{schema}' to see all available tables"
        }


# ---------------------------------------------------------
# Tool: portfolio_query - Query portfolio data with structured parameters
# ---------------------------------------------------------

@mcp.tool()
def portfolio_query(
    from_month: str,
    to_month: str,
    group_by: list[str],
    metrics: list[str],
    product_name: str = None,
    filters: dict = None
) -> dict:
    """
    Query portfolio data from collections_portfolio.monthly_snapshot table.
    This tool prevents raw SQL injection by accepting only structured parameters.
    
    Args:
        from_month: Start month in YYYY-MM format (e.g., "2025-07")
        to_month: End month in YYYY-MM format (e.g., "2025-09")
        group_by: List of dimensions to group by. Allowed values: ["file_month", "region", "bucket", "vintage_band"]
        metrics: List of metrics to calculate. Allowed values: ["pos", "one_plus_balance", "recovery_rate", "flow_rate_b1_b2", "rate_loss_value", "rate_loss_bps"]
        product_name: Optional product name filter (e.g., "PL Self")
        filters: Optional dict of additional filters (e.g., {"bucket": "B2", "region": "West"})
    
    Returns:
        Dictionary with query results containing columns and rows
    """
    # Validate required parameters are not empty
    if not group_by:
        return {
            "error": "group_by cannot be empty. Provide at least one dimension to group by.",
            "allowed_values": ["file_month", "region", "bucket", "vintage_band"]
        }
    
    if not metrics:
        return {
            "error": "metrics cannot be empty. Provide at least one metric to calculate.",
            "allowed_values": ["pos", "one_plus_balance", "recovery_rate", "flow_rate_b1_b2", "rate_loss_value", "rate_loss_bps"]
        }
    
    # Validate group_by values
    allowed_group_by = ["file_month", "region", "bucket", "vintage_band"]
    invalid_group_by = [g for g in group_by if g not in allowed_group_by]
    if invalid_group_by:
        return {
            "error": f"Invalid group_by values: {invalid_group_by}. Allowed values: {allowed_group_by}",
            "allowed_values": allowed_group_by
        }
    
    # Validate metrics
    allowed_metrics = ["pos", "one_plus_balance", "recovery_rate", "flow_rate_b1_b2", "rate_loss_value", "rate_loss_bps"]
    invalid_metrics = [m for m in metrics if m not in allowed_metrics]
    if invalid_metrics:
        return {
            "error": f"Invalid metrics: {invalid_metrics}. Allowed values: {allowed_metrics}",
            "allowed_values": allowed_metrics
        }
    
    # Validate date format (YYYY-MM)
    date_pattern = r"^\d{4}-\d{2}$"
    if not re.match(date_pattern, from_month):
        return {"error": f"Invalid from_month format: {from_month}. Expected YYYY-MM format"}
    if not re.match(date_pattern, to_month):
        return {"error": f"Invalid to_month format: {to_month}. Expected YYYY-MM format"}
    
    # Define aggregation functions for each metric
    # SUM for additive metrics, AVG for rates
    aggregation_map = {
        "pos": "SUM",
        "one_plus_balance": "SUM",
        "recovery_rate": "AVG",
        "flow_rate_b1_b2": "AVG",
        "rate_loss_value": "SUM",
        "rate_loss_bps": "AVG"
    }
    
    conn = get_conn()
    cur = conn.cursor()
    
    try:
        # Build SELECT clause - include group_by columns and aggregated metrics
        select_parts = list(group_by)
        for metric in metrics:
            agg_func = aggregation_map.get(metric, "SUM")
            select_parts.append(f"{agg_func}({metric}) AS {metric}")
        
        select_clause = ", ".join(select_parts)
        
        # Build WHERE clause
        where_conditions = [
            "file_month >= %s",
            "file_month <= %s"
        ]
        params = [from_month, to_month]
        
        # Add product_name filter if provided
        if product_name:
            where_conditions.append("product_name = %s")
            params.append(product_name)
        
        # Add additional filters if provided
        if filters:
            # Validate filter keys against allowed group_by values
            allowed_filters = allowed_group_by + ["product_name"]
            for filter_key, filter_value in filters.items():
                # Skip product_name if it's already handled as a parameter
                if filter_key == "product_name" and product_name:
                    continue
                if filter_key in allowed_filters:
                    # Only add filter if value is not None
                    if filter_value is not None and filter_value != "":
                        where_conditions.append(f"{filter_key} = %s")
                        params.append(filter_value)
                else:
                    # Skip invalid filter keys
                    pass
        
        where_clause = " AND ".join(where_conditions)
        
        # Build GROUP BY clause
        group_by_clause = ", ".join(group_by) if group_by else "1"
        
        # Build final SQL query
        sql = f"""
            SELECT {select_clause}
            FROM collections_portfolio.monthly_snapshot
            WHERE {where_clause}
            GROUP BY {group_by_clause}
            ORDER BY {group_by_clause}
        """
        
        # Execute query
        cur.execute(sql, params)
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        # Fetch all rows
        rows = cur.fetchall()
        
        # Convert rows to list of dicts for easier consumption
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
        cur.close()
        conn.close()
        
        return {
            "columns": columns,
            "rows": results,
            "row_count": len(results),
            "query_params": {
                "from_month": from_month,
                "to_month": to_month,
                "product_name": product_name,
                "group_by": group_by,
                "metrics": metrics,
                "filters": filters
            }
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        
        return {
            "error": str(e),
            "message": "Error executing portfolio query. Check table exists and parameters are correct."
        }


# ---------------------------------------------------------
# Run server
# ---------------------------------------------------------

if __name__ == "__main__":
    mcp.run()

