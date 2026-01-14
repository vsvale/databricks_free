CREATE MATERIALIZED VIEW IF NOT EXISTS quality.default.columns
CLUSTER BY AUTO
COMMENT 'columns hist from system.information_schema.columns'
TRIGGER ON UPDATE
TBLPROPERTIES(pipelines.channel = "PREVIEW")
AS
SELECT *, current_timestamp() as ts from system.information_schema.columns
where table_catalog not in ("databricks_databricks_documentation_dataset","samples","system") and table_schema not in ("information_schema")