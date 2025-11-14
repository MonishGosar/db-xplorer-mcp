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

### 1. Install Dependencies
```powershell
pip install fastmcp psycopg2-binary
```

### 2. Set Environment Variables
Create a `.env` file or set in PowerShell:
```powershell
$env:DB_HOST="your-database-host"
$env:DB_PORT="5432"
$env:DB_NAME="your-database-name"
$env:DB_USER="your-username"
$env:DB_PASSWORD="your-password"
```

### 3. Configure Cursor MCP

**Option A: Via Cursor Settings UI**
1. Open Cursor Settings (`Ctrl+,`)
2. Go to `Features > MCP Servers`
3. Click "Add new MCP server"
4. Use the config from `cursor-mcp-config.json` (update paths and credentials)

**Option B: Edit Config File Directly**
1. Open: `%APPDATA%\Cursor\User\globalStorage\mcp.json`
2. Add the configuration from `cursor-mcp-config.json`
3. Update database credentials
4. Restart Cursor

See `CURSOR_SETUP.md` for detailed instructions!

## Environment Variables

Required environment variables:

```
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password
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

## Example Queries in Cursor

Once deployed, try these in Cursor chat:

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

