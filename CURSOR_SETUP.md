# Deploying db-xplorer MCP in Cursor

## Method 1: Local Deployment (Recommended for Development)

### Step 1: Install Dependencies

Open a terminal in the `db-xplorer` folder and install dependencies:

```powershell
cd "C:\Users\monis\OneDrive\Desktop\Indilabs.ai\Chatbot 2.0\db-xplorer"
pip install fastmcp psycopg2-binary
```

### Step 2: Set Environment Variables

Create a `.env` file in the `db-xplorer` folder (or set them in your system):

```env
DB_HOST=your-database-host
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-username
DB_PASSWORD=your-password
```

**OR** set them in PowerShell before running:
```powershell
$env:DB_HOST="your-database-host"
$env:DB_PORT="5432"
$env:DB_NAME="your-database-name"
$env:DB_USER="your-username"
$env:DB_PASSWORD="your-password"
```

### Step 3: Test the Server Locally

Test that the server runs correctly:

```powershell
python server.py
```

If it runs without errors, press `Ctrl+C` to stop it.

### Step 4: Configure Cursor MCP

1. **Open Cursor Settings:**
   - Press `Ctrl+,` (or `Cmd+,` on Mac)
   - Or go to `File > Preferences > Settings`

2. **Navigate to MCP Settings:**
   - Search for "MCP" in settings
   - Or go to `Features > MCP Servers`

3. **Add New MCP Server:**
   - Click "Add new MCP server" or "Edit MCP Settings"
   - Add this configuration:

```json
{
  "mcpServers": {
    "db-xplorer": {
      "command": "python",
      "args": [
        "C:\\Users\\monis\\OneDrive\\Desktop\\Indilabs.ai\\Chatbot 2.0\\db-xplorer\\server.py"
      ],
      "env": {
        "DB_HOST": "your-database-host",
        "DB_PORT": "5432",
        "DB_NAME": "your-database-name",
        "DB_USER": "your-username",
        "DB_PASSWORD": "your-password"
      }
    }
  }
}
```

**Important:** Replace the paths and database credentials with your actual values!

### Step 5: Restart Cursor

- Close and reopen Cursor for the MCP server to be loaded

### Step 6: Verify It's Working

In Cursor's chat, try:
- "List all schemas in the database"
- "What tables are in the collections schema?"
- "Analyze data for prediction in november 2023"

The MCP tools should be available automatically!

---

## Method 2: Using Cursor's MCP Settings File

Cursor stores MCP configuration in a settings file. You can edit it directly:

### Windows Location:
```
%APPDATA%\Cursor\User\globalStorage\mcp.json
```

Or:
```
C:\Users\monis\AppData\Roaming\Cursor\User\globalStorage\mcp.json
```

### Add this configuration:

```json
{
  "mcpServers": {
    "db-xplorer": {
      "command": "python",
      "args": [
        "C:\\Users\\monis\\OneDrive\\Desktop\\Indilabs.ai\\Chatbot 2.0\\db-xplorer\\server.py"
      ],
      "env": {
        "DB_HOST": "your-database-host",
        "DB_PORT": "5432",
        "DB_NAME": "your-database-name",
        "DB_USER": "your-username",
        "DB_PASSWORD": "your-password"
      }
    }
  }
}
```

---

## Method 3: Using FastMCP Cloud (Production)

If you've deployed to FastMCP Cloud, you can connect to it via HTTP:

```json
{
  "mcpServers": {
    "db-xplorer-cloud": {
      "url": "https://your-fastmcp-cloud-url.com",
      "headers": {
        "Authorization": "Bearer your-api-key"
      }
    }
  }
}
```

---

## Troubleshooting

### Issue: MCP server not starting
- **Solution:** Check that Python is in your PATH: `python --version`
- **Solution:** Verify the path to `server.py` is correct
- **Solution:** Check environment variables are set correctly

### Issue: Database connection errors
- **Solution:** Verify database credentials are correct
- **Solution:** Check database is accessible from your machine
- **Solution:** Test connection with: `python -c "from server import get_conn; get_conn()"`

### Issue: Tools not appearing in Cursor
- **Solution:** Restart Cursor completely
- **Solution:** Check Cursor's MCP logs: `View > Output > MCP`
- **Solution:** Verify the server starts without errors

### Issue: Permission errors
- **Solution:** Run Cursor as administrator (if needed)
- **Solution:** Check file permissions on `server.py`

---

## Available Tools

Once deployed, these tools will be available in Cursor:

1. **list_schemas** - List all schemas
2. **list_tables** - List tables in a schema
3. **describe_table** - Get table details
4. **search_columns** - Search for columns
5. **preview_rows** - Preview table data
6. **get_row_count** - Get row counts
7. **run_query_safe** - Run safe SELECT queries
8. **smart_search** - Comprehensive search
9. **deep_search** - Search inside table data
10. **analyze_data** - Natural language analysis
11. **find_data_by_value** - Find data by value

---

## Quick Test

After setup, try this in Cursor chat:

```
Use the analyze_data tool to find information about prediction data for november 2023
```

Or:

```
List all schemas in the database and show me tables in the first schema
```

