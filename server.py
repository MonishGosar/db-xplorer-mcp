import os
import psycopg2
import re
from fastmcp import MCPServer, tool


# ---------------------------------------------------------
# DB Connection
# ---------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD")
    )


server = MCPServer("db-xplorer")


# ---------------------------------------------------------
# Tool: list_schemas
# ---------------------------------------------------------

@tool
def list_schemas() -> dict:
    conn = get_conn()
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
    conn.close()

    return {"schemas": schemas}


# ---------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------

@tool
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
    except:
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
    for table_name, estimate in cur.fetchall():
        result.append({
            "table_name": table_name,
            "row_estimate": int(estimate),
            "description": desc_map.get(table_name, "")
        })

    cur.close()
    conn.close()
    return {"tables": result}


# ---------------------------------------------------------
# Tool: describe_table
# ---------------------------------------------------------

@tool
def describe_table(schema: str, table: str) -> dict:
    conn = get_conn()
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
    except:
        description = ""
        grain = ""

    # all columns
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position;
    """, (schema, table))

    columns = cur.fetchall()

    # dimensions/measures metadata
    try:
        cur.execute("""
            SELECT column_name, description, role
            FROM metadata.column_description
            WHERE schema_name=%s AND table_name=%s;
        """, (schema, table))

        meta = cur.fetchall()
    except:
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

@tool
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
    except:
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
# Tool: preview_rows
# ---------------------------------------------------------

@tool
def preview_rows(schema: str, table: str, limit: int = 20) -> dict:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        f"SELECT * FROM {schema}.{table} LIMIT %s;",
        (limit,)
    )

    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return {"columns": cols, "rows": rows}


# ---------------------------------------------------------
# Tool: get_row_count
# ---------------------------------------------------------

@tool
def get_row_count(schema: str, table: str) -> dict:
    conn = get_conn()
    cur = conn.cursor()

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
    except:
        exact = None

    cur.close()
    conn.close()

    return {"row_estimate": estimate, "row_exact": exact}


# ---------------------------------------------------------
# Tool: run_query_safe
# ---------------------------------------------------------

FORBIDDEN = ["update", "delete", "insert", "alter", "drop", "truncate", "create"]

BLOCK_PATTERNS = [
    r"cross join",
    r",\s*\w+\s*"
]


@tool
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
        cur.close()
        conn.close()
        return {"error": str(e)}

    cur.close()
    conn.close()

    return {"columns": cols, "rows": rows}


# ---------------------------------------------------------
# Run server
# ---------------------------------------------------------

if __name__ == "__main__":
    server.run()

