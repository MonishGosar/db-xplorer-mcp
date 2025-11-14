import os
import psycopg2
import re
from fastmcp import FastMCP


# ---------------------------------------------------------
# DB Connection
# ---------------------------------------------------------

def get_conn():
    def clean_env_var(value, default=None):
        """Strip quotes and whitespace from environment variable values"""
        if value is None:
            return default
        # Remove surrounding quotes (single or double) and strip whitespace
        value = value.strip().strip('"').strip("'")
        return value if value else default
    
    def get_port():
        port = clean_env_var(os.environ.get("DB_PORT"), "5432")
        try:
            return int(port)
        except (ValueError, TypeError):
            return 5432
    
    conn = psycopg2.connect(
        host=clean_env_var(os.environ.get("DB_HOST")),
        port=get_port(),
        dbname=clean_env_var(os.environ.get("DB_NAME")),
        user=clean_env_var(os.environ.get("DB_USER")),
        password=clean_env_var(os.environ.get("DB_PASSWORD"))
    )
    # Enable autocommit for read-only queries to avoid transaction aborted errors
    conn.autocommit = True
    return conn


mcp = FastMCP("db-xplorer")


# ---------------------------------------------------------
# Tool: list_schemas
# ---------------------------------------------------------

@mcp.tool()
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

@mcp.tool()
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
# Tool: preview_rows
# ---------------------------------------------------------

@mcp.tool()
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

@mcp.tool()
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
    except Exception:
        # Rollback if transaction was aborted
        try:
            conn.rollback()
        except Exception:
            pass
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
        cur.close()
        conn.close()
        return {"error": str(e)}

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
# Tool: analyze_data - Natural language query analysis
# ---------------------------------------------------------

@mcp.tool()
def analyze_data(query: str, date_filter: str = None) -> dict:
    """
    Analyze data based on natural language queries.
    Example: "what is precision for nov 23" will search for precision-related data
    in November 2023. Automatically searches schemas, tables, and columns.
    """
    conn = get_conn()
    cur = conn.cursor()
    
    query_lower = query.lower()
    
    # Extract key terms from query
    keywords = []
    for word in query_lower.split():
        if len(word) > 3:  # Ignore short words like "the", "for", etc.
            keywords.append(word)
    
    # Extract date information
    date_patterns = {
        "nov": "11", "november": "11",
        "dec": "12", "december": "12",
        "jan": "01", "january": "01",
        "feb": "02", "february": "02",
        "mar": "03", "march": "03",
        "apr": "04", "april": "04",
        "may": "05",
        "jun": "06", "june": "06",
        "jul": "07", "july": "07",
        "aug": "08", "august": "08",
        "sep": "09", "september": "09",
        "oct": "10", "october": "10"
    }
    
    month = None
    year = None
    for key, value in date_patterns.items():
        if key in query_lower:
            month = value
            break
    
    # Try to extract year (23, 2023, etc.)
    year_match = re.search(r'\b(20\d{2}|\d{2})\b', query_lower)
    if year_match:
        year_str = year_match.group(1)
        year = f"20{year_str}" if len(year_str) == 2 else year_str
    
    results = {
        "query": query,
        "extracted_keywords": keywords,
        "date_info": {
            "month": month,
            "year": year,
            "date_filter": date_filter
        },
        "found_tables": [],
        "found_columns": [],
        "sample_data": []
    }
    
    try:
        # First, do a smart search to find relevant tables/columns
        search_query = " ".join(keywords[:3])
        query_pattern = f"%{search_query.lower()}%"
        
        # Search tables
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND (table_schema ILIKE %s OR table_name ILIKE %s)
            ORDER BY table_schema, table_name
            LIMIT 30;
        """, (query_pattern, query_pattern))
        results["found_tables"] = [{"schema": row[0], "table": row[1]} for row in cur.fetchall()]
        
        # Search columns
        cur.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            AND column_name ILIKE %s
            ORDER BY table_schema, table_name, column_name
            LIMIT 50;
        """, (query_pattern,))
        results["found_columns"] = [
            {
                "schema": row[0],
                "table": row[1],
                "column": row[2],
                "data_type": row[3]
            }
            for row in cur.fetchall()
        ]
        
        # Try to find date columns
        date_columns = []
        for col_info in results["found_columns"]:
            col_name = col_info.get("column", "").lower()
            if any(term in col_name for term in ["date", "time", "created", "updated", "month", "year"]):
                date_columns.append(col_info)
        
        results["date_columns_found"] = date_columns
        
        # If we found relevant tables, try to query them
        if results["found_tables"]:
            # Take the first few relevant tables
            for table_info in results["found_tables"][:3]:
                schema = table_info["schema"]
                table = table_info["table"]
                
                try:
                    # Build a query to search for the keywords
                    cur.execute(f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        LIMIT 20;
                    """, (schema, table))
                    
                    columns = cur.fetchall()
                    col_names = [col[0] for col in columns]
                    
                    if col_names:
                        # Try to get sample data
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
                    continue
        
        cur.close()
        conn.close()
        
        return {
            "analysis": results,
            "recommendations": [
                f"Found {len(results['found_tables'])} relevant tables",
                f"Found {len(results['found_columns'])} relevant columns",
                "Use 'preview_rows' or 'run_query_safe' to explore specific tables"
            ]
        }
        
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        cur.close()
        conn.close()
        return {"error": str(e), "partial_results": results}


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
# Run server
# ---------------------------------------------------------

if __name__ == "__main__":
    mcp.run()

