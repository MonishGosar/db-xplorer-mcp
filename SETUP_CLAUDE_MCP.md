# How to Configure Claude (Cursor AI) to Use Your MCP Server

This guide will help you set up the `portfolio_query` MCP tool so that Claude (Cursor's AI assistant) can query your portfolio data.

## Prerequisites

1. **Python installed** (Python 3.8+)
2. **Cursor IDE** installed
3. **Database access** credentials

## Step-by-Step Setup

### Step 1: Install Dependencies

Open PowerShell in the `DB-Xplorer` folder:

```powershell
cd "C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\DB-Xplorer"
pip install fastmcp psycopg2-binary
```

Verify installation:
```powershell
python --version
pip list | findstr fastmcp
```

### Step 2: Update Database Credentials

Edit `cursor-mcp-config.json` and replace the placeholder values:

```json
{
  "mcpServers": {
    "db-xplorer": {
      "command": "python",
      "args": [
        "C:\\Users\\monis\\OneDrive\\Desktop\\Indilabs.ai\\Chatbot 2.0\\DB-Xplorer\\server.py"
      ],
      "env": {
        "DB_HOST": "your-actual-database-host",
        "DB_PORT": "5432",
        "DB_NAME": "your-actual-database-name",
        "DB_USER": "your-actual-username",
        "DB_PASSWORD": "your-actual-password"
      }
    }
  }
}
```

**Important:** Replace all `YOUR_DB_*_HERE` values with your actual database credentials!

### Step 3: Test the MCP Server Locally

Test that the server runs without errors:

```powershell
cd "C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\DB-Xplorer"
python server.py
```

If it starts successfully, you'll see the server running. Press `Ctrl+C` to stop it.

### Step 4: Configure Cursor MCP Settings

You have two options to configure Cursor:

#### Option A: Via Cursor Settings UI (Recommended)

1. **Open Cursor Settings:**
   - Press `Ctrl+,` (Windows) or `Cmd+,` (Mac)
   - Or go to `File > Preferences > Settings`

2. **Navigate to MCP Settings:**
   - In the settings search bar, type: `MCP`
   - Or go to `Features > MCP Servers` in the settings sidebar

3. **Add MCP Server Configuration:**
   - Click "Add new MCP server" or "Edit MCP Settings"
   - Copy the entire content from `cursor-mcp-config.json`
   - Paste it into the MCP settings JSON editor
   - **Make sure to update the database credentials!**

4. **Save the settings**

#### Option B: Edit MCP Config File Directly

1. **Locate the MCP config file:**
   ```
   %APPDATA%\Cursor\User\globalStorage\mcp.json
   ```
   
   Or the full path:
   ```
   C:\Users\monis\AppData\Roaming\Cursor\User\globalStorage\mcp.json
   ```

2. **Open the file in a text editor**

3. **Add or update the configuration:**
   - If the file doesn't exist, create it with this structure:
   ```json
   {
     "mcpServers": {
       "db-xplorer": {
         "command": "python",
         "args": [
           "C:\\Users\\monis\\OneDrive\\Desktop\\Indilabs.ai\\Chatbot 2.0\\DB-Xplorer\\server.py"
         ],
         "env": {
           "DB_HOST": "your-actual-database-host",
           "DB_PORT": "5432",
           "DB_NAME": "your-actual-database-name",
           "DB_USER": "your-actual-username",
           "DB_PASSWORD": "your-actual-password"
         }
       }
     }
   }
   ```
   
   - If the file already exists with other MCP servers, add the `db-xplorer` entry inside the `mcpServers` object

4. **Save the file**

### Step 5: Restart Cursor

**Important:** You must restart Cursor completely for the MCP server to load:

1. Close all Cursor windows
2. Wait a few seconds
3. Reopen Cursor

### Step 6: Verify MCP Server is Running

1. **Check MCP Logs:**
   - Go to `View > Output` in Cursor
   - Select "MCP" from the dropdown
   - Look for messages indicating the `db-xplorer` server has started
   - You should see: `"db-xplorer" server started` or similar

2. **Test in Cursor Chat:**
   Open Cursor's chat (Ctrl+L) and try:
   ```
   List all schemas in the database
   ```
   
   Or:
   ```
   What tables are available in the collections_portfolio schema?
   ```

### Step 7: Test Portfolio Query Tool

Once the MCP server is running, test the `portfolio_query` tool:

**Example 1: Basic Query**
```
How is PL Self doing in B2 over the last 3 months?
```

**Example 2: Rate Loss Query**
```
How has rate loss changed in PL Self B2 in West in last 3 months?
```

**Example 3: Recovery Rate Query**
```
Show me recovery rate trends for PL Self by region for the last quarter
```

Claude should automatically:
1. Recognize the question is about portfolio data
2. Call the `portfolio_query` tool with appropriate parameters
3. Parse the date range (e.g., "last 3 months" → calculate from_month/to_month)
4. Extract filters (product_name, bucket, region)
5. Select appropriate metrics and group_by dimensions
6. Return a summary with insights

## Available MCP Tools

Once configured, these tools are available to Claude:

### Portfolio Query Tool
- **portfolio_query** - Query portfolio data with structured parameters
  - Parameters: `from_month`, `to_month`, `group_by`, `metrics`, `product_name`, `filters`
  - Returns: Structured portfolio data with aggregations

### Database Exploration Tools
- **list_schemas** - List all schemas
- **list_tables** - List tables in a schema
- **describe_table** - Get table details
- **search_columns** - Search for columns
- **preview_rows** - Preview table data
- **get_row_count** - Get row counts
- **run_query_safe** - Run safe SELECT queries
- **smart_search** - Comprehensive search
- **analyze_data** - Natural language analysis

## Troubleshooting

### Issue: MCP Server Not Starting

**Symptoms:**
- Tools not appearing in Cursor chat
- Errors in MCP logs

**Solutions:**
1. **Check Python is in PATH:**
   ```powershell
   python --version
   ```
   If not found, add Python to your system PATH

2. **Verify server path is correct:**
   - Check the path in `cursor-mcp-config.json` matches your actual file location
   - Use forward slashes or escaped backslashes: `C:\\Users\\...`

3. **Test server manually:**
   ```powershell
   cd "C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\DB-Xplorer"
   python server.py
   ```
   If errors appear, fix them before configuring in Cursor

### Issue: Database Connection Errors

**Symptoms:**
- "Connection refused" errors
- "Authentication failed" errors

**Solutions:**
1. **Verify database credentials:**
   - Check `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` in config
   - Test connection with a database client

2. **Check database accessibility:**
   - Ensure database is running
   - Check firewall settings
   - Verify network connectivity

3. **Test connection manually:**
   ```powershell
   python -c "from server import get_conn; conn = get_conn(); print('Connected!'); conn.close()"
   ```

### Issue: Tools Not Appearing in Chat

**Symptoms:**
- Claude doesn't use MCP tools
- No tools available in suggestions

**Solutions:**
1. **Restart Cursor completely:**
   - Close all windows
   - Wait a few seconds
   - Reopen Cursor

2. **Check MCP logs:**
   - Go to `View > Output > MCP`
   - Look for error messages
   - Verify server started successfully

3. **Verify MCP config:**
   - Check `mcp.json` file is valid JSON
   - Ensure no syntax errors
   - Verify paths are correct

4. **Clear Cursor cache:**
   - Close Cursor
   - Delete cache files (if needed)
   - Reopen Cursor

### Issue: Portfolio Query Not Working

**Symptoms:**
- Claude doesn't recognize portfolio queries
- Tool returns errors

**Solutions:**
1. **Check table exists:**
   - Verify `collections_portfolio.monthly_snapshot` table exists
   - Test with: `list_tables` tool for `collections_portfolio` schema

2. **Verify column names:**
   - Check table has required columns: `file_month`, `product_name`, `region`, `bucket`, `vintage_band`
   - Check metrics columns exist: `pos`, `one_plus_balance`, `recovery_rate`, etc.

3. **Test query manually:**
   - Use `run_query_safe` tool to test a simple query
   - Verify date format is `YYYY-MM`

4. **Check date format:**
   - Ensure dates are in `YYYY-MM` format (e.g., "2025-07")
   - Claude should automatically convert "last 3 months" to proper format

## Example Conversations

### Example 1: Basic Portfolio Query

**You:**
```
How is PL Self performing in bucket B2 over the last 3 months?
```

**Claude should:**
1. Call `portfolio_query` with:
   - `product_name`: "PL Self"
   - `from_month`: "2025-07" (calculated from current date)
   - `to_month`: "2025-09" (calculated from current date)
   - `group_by`: ["file_month"]
   - `metrics`: ["pos", "recovery_rate", "rate_loss_value"]
   - `filters`: {"bucket": "B2"}

2. Return insights like:
   - "PL Self in B2 shows increasing POS from X to Y"
   - "Recovery rate improved by Z%"
   - "Rate loss decreased by W bps"

### Example 2: Regional Analysis

**You:**
```
Show me recovery rates for PL Self by region in the last quarter
```

**Claude should:**
1. Call `portfolio_query` with:
   - `product_name`: "PL Self"
   - `group_by`: ["file_month", "region"]
   - `metrics`: ["recovery_rate"]
   - Appropriate date range

2. Return regional comparison with trends

### Example 3: Rate Loss Analysis

**You:**
```
How has rate loss changed in PL Self B2 in West region in last 3 months?
```

**Claude should:**
1. Call `portfolio_query` with:
   - `product_name`: "PL Self"
   - `filters`: {"bucket": "B2", "region": "West"}
   - `metrics`: ["rate_loss_value", "rate_loss_bps"]
   - `group_by`: ["file_month"]

2. Return trend analysis with percentage changes

## Next Steps

Once the MCP server is working:

1. **Test various queries** to ensure Claude understands the portfolio data
2. **Fine-tune system prompts** if needed for better analysis
3. **Add more tools** if needed for additional functionality
4. **Monitor MCP logs** for any issues

## Support

If you encounter issues:
1. Check MCP logs in Cursor (`View > Output > MCP`)
2. Test the server manually with `python server.py`
3. Verify database connection
4. Check Cursor documentation for MCP setup

## Quick Reference

**MCP Config File Location:**
```
%APPDATA%\Cursor\User\globalStorage\mcp.json
```

**Server Path:**
```
C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\DB-Xplorer\server.py
```

**Test Server:**
```powershell
cd "C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\DB-Xplorer"
python server.py
```

**Check MCP Status:**
- `View > Output > MCP` in Cursor
- Look for "db-xplorer" server started message



