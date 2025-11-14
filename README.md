# db-xplorer (FastMCP)

A lightweight DB explorer MCP that lets LLMs understand your DB:

- schemas
- tables
- descriptions
- columns
- dimensions vs measures
- sample rows
- safe SQL query execution

## Environment Variables

Set these in FastMCP Cloud:

```
DB_HOST=
DB_PORT=5432
DB_NAME=
DB_USER=
DB_PASSWORD=
```

## Deploy Steps (FastMCP Cloud)

1. Zip the folder:
   ```
   db-xplorer.zip
   ```

2. Upload to FastMCP Cloud dashboard

3. Set environment variables

4. Deploy

## Tools available

- list_schemas
- list_tables
- describe_table
- search_columns
- preview_rows
- get_row_count
- run_query_safe

## Notes

- Only SELECT allowed
- All queries autoforce LIMIT 200
- Cross joins blocked

