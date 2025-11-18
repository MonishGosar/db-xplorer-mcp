# db-xplorer (FastMCP)

A lightweight DB explorer MCP that lets LLMs understand your DB:

- schemas
- tables
- descriptions
- columns
- dimensions vs measures
- sample rows
- safe SQL query execution
- intelligent search and analysis

## Quick Start: Deploy in Cursor

### 🚀 5-Minute Setup

See **[QUICK_START.md](QUICK_START.md)** for a quick setup guide.

### 📖 Detailed Instructions

See **[SETUP_CLAUDE_MCP.md](SETUP_CLAUDE_MCP.md)** for step-by-step instructions on configuring Claude (Cursor AI) to use the MCP server.

### 📚 Full Documentation

See **[CURSOR_SETUP.md](CURSOR_SETUP.md)** for comprehensive setup documentation and troubleshooting.

### Basic Setup Steps

1. **Install Dependencies:**
   ```powershell
   pip install fastmcp psycopg2-binary
   ```

2. **Update Database Credentials:**
   - Edit `cursor-mcp-config.json`
   - Replace `YOUR_DB_*_HERE` with your actual database credentials

3. **Configure Cursor MCP:**
   - Open Cursor Settings (`Ctrl+,`)
   - Go to `Features > MCP Servers`
   - Add the configuration from `cursor-mcp-config.json`
   - **Or** edit `%APPDATA%\Cursor\User\globalStorage\mcp.json` directly

4. **Restart Cursor:**
   - Close and reopen Cursor completely

5. **Verify It Works:**
   - Check MCP logs: `View > Output > MCP`
   - Test in chat: `List all schemas in the database`

## Environment Variables

Required environment variables:

```
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password
# Optional performance tuning
DB_POOL_MIN=1
DB_POOL_MAX=5
CACHE_TTL_SECONDS=300
```

## Deploy Steps (FastMCP Cloud)

1. Zip the folder:
   ```
   db-xplorer.zip
   ```

2. Upload to FastMCP Cloud dashboard

3. Set environment variables

4. Deploy

## Tools Available

### Basic Tools
- **list_schemas** - List all interesting schemas
- **list_tables** - List tables in a schema with row estimates
- **describe_table** - Get detailed table information
- **search_columns** - Search for columns by name/description
- **preview_rows** - Preview table data
- **get_row_count** - Get row counts (estimate/exact)
- **run_query_safe** - Run safe SELECT queries

### Intelligent Tools
- **smart_search** - Comprehensive search across everything
- **deep_search** - Search inside table data
- **analyze_data** - Natural language query analysis (handles typos, variations)
- **find_data_by_value** - Find tables containing specific values

### Portfolio Query Tool (v0 - CRO Bot)
- **portfolio_query** - Query portfolio data from `collections_portfolio.monthly_snapshot` table with structured parameters. This tool prevents SQL injection by accepting only validated structured inputs.
  
  **Parameters:**
  - `from_month` (required): Start month in YYYY-MM format (e.g., "2025-07")
  - `to_month` (required): End month in YYYY-MM format (e.g., "2025-09")
  - `group_by` (required): List of dimensions to group by. Allowed: `["file_month", "region", "bucket", "vintage_band"]`
  - `metrics` (required): List of metrics to calculate. Allowed: `["pos", "one_plus_balance", "recovery_rate", "flow_rate_b1_b2", "rate_loss_value", "rate_loss_bps"]`
  - `product_name` (optional): Product name filter (e.g., "PL Self")
  - `filters` (optional): Dict of additional filters (e.g., `{"bucket": "B2", "region": "West"}`)
  
  **Example:**
  ```python
  portfolio_query(
      from_month="2025-07",
      to_month="2025-09",
      group_by=["file_month"],
      metrics=["rate_loss_value", "rate_loss_bps"],
      product_name="PL Self",
      filters={"bucket": "B2", "region": "West"}
  )
  ```
  
  **Safety Features:**
  - No raw SQL - only structured parameters accepted
  - All column names validated against allowlist
  - Parameterized queries prevent SQL injection
  - Proper aggregation (SUM for additive metrics, AVG for rates)

## Example Queries in Cursor

Once deployed, try these in Cursor chat:

### Portfolio Query Examples

```
How has rate loss changed in PL Self B2 in West in last 3 months?
```

```
How is PL Self doing in B2 over the last 3 months?
```

```
Show me recovery rate trends for PL Self by region for the last quarter
```

### General Database Exploration

```
Analyze data for prediction metrics in november 2023
```

```
List all schemas and show me tables in the collections schema
```

```
Search for precision-related columns across all tables
```

```
What is the row count for the metrics table?
```

## Notes

- Only SELECT queries allowed in `run_query_safe`
- All queries auto-force LIMIT 200
- Cross joins blocked for safety
- Handles typos and variations in natural language queries
- Multiple search strategies with fallbacks

