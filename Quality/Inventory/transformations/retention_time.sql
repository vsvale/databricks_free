CREATE MATERIALIZED VIEW IF NOT EXISTS quality.default.retention_time (
    full_table_name STRING NOT NULL,
    catalog_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    tab_name STRING NOT NULL,
    retention_time TIMESTAMP,
    CONSTRAINT retention_time_full_table_name_pk PRIMARY KEY(full_table_name)
)
CLUSTER BY AUTO
COMMENT 'retention time per table'
TBLPROPERTIES(pipelines.channel = "PREVIEW")
AS
SELECT DISTINCT
    CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_table_name,
    table_catalog AS catalog_name,
    table_schema AS schema_name,
    table_name AS tab_name,
    created + INTERVAL 5 YEARS AS retention_time
FROM system.information_schema.tables
WHERE
    table_catalog NOT IN ('system', 'samples')
    AND table_schema NOT IN ('information_schema')