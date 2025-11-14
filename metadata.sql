CREATE SCHEMA IF NOT EXISTS metadata;


CREATE TABLE IF NOT EXISTS metadata.table_description (
    schema_name TEXT,
    table_name TEXT,
    description TEXT,
    grain TEXT,
    PRIMARY KEY (schema_name, table_name)
);


CREATE TABLE IF NOT EXISTS metadata.column_description (
    schema_name TEXT,
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    description TEXT,
    role TEXT,  -- 'dimension' or 'measure'
    PRIMARY KEY (schema_name, table_name, column_name)
);

