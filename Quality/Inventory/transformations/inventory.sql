CREATE MATERIALIZED VIEW IF NOT EXISTS quality.default.inventory()
CLUSTER BY AUTO
COMMENT 'Comprehensive inventory of Unity Catalog tables, including metadata, retention, and deprecation status'
TRIGGER ON UPDATE
TBLPROPERTIES(pipelines.channel = "PREVIEW")
AS
SELECT 
    t.table_catalog,
    t.table_schema,
    t.table_name,
    CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) AS full_table_name,
    t.table_owner,
    t.table_type,
    CASE WHEN t.is_insertable_into = 'YES' THEN TRUE ELSE FALSE END AS is_insertable_into,
    t.comment,
    t.created,
    t.created_by,
    t.last_altered,
    t.last_altered_by,
    tla.last_access,
    rt.retention_time,
    t.storage_path,
    ddt.clusteringColumns,
    ddt.numFiles,
    (ddt.sizeInBytes) / 1024 / 1024/ 1024 AS sizeInGB,
    ddt.properties,
    ddt.id,
    ddt.tableFeatures,
    CASE 
        WHEN t.last_altered <= current_date() - INTERVAL 60 DAYS 
          OR tla.last_access <= current_date() - INTERVAL 60 DAYS 
          OR current_date() >= rt.retention_time 
        THEN TRUE 
        ELSE FALSE 
    END AS to_be_deprecated
FROM system.information_schema.tables AS t
LEFT JOIN quality.default.table_last_access AS tla
    ON t.table_catalog = tla.catalog_name
    AND t.table_schema = tla.schema_name
    AND t.table_name = tla.tab_name
LEFT JOIN quality.default.retention_time AS rt
    ON t.table_catalog = rt.catalog_name
    AND t.table_schema = rt.schema_name
    AND t.table_name = rt.tab_name
LEFT JOIN quality.default.describe_details_tables AS ddt
ON t.table_catalog = ddt.table_catalog
    AND t.table_schema = ddt.table_schema
    AND t.table_name = ddt.table_name
WHERE 
    t.table_schema NOT IN ('information_schema') 
    AND t.table_catalog NOT IN ('system', 'samples')