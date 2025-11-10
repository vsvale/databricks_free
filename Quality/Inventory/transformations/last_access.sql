CREATE MATERIALIZED VIEW IF NOT EXISTS quality.default.table_last_access(
    full_table_name STRING NOT NULL,
    catalog_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    tab_name STRING NOT NULL,
    last_access TIMESTAMP NOT NULL,
    CONSTRAINT table_last_access_full_table_name_pk PRIMARY KEY(full_table_name)
)
CLUSTER BY AUTO
COMMENT 'last access per table'
TRIGGER ON UPDATE
TBLPROPERTIES(pipelines.channel = "PREVIEW")
AS
WITH all_data AS (
  SELECT
    COALESCE(source_table_full_name, target_table_full_name) AS fqn,
    event_time,
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(source_table_full_name, target_table_full_name)
      ORDER BY event_time DESC
    ) AS rn
  FROM system.access.table_lineage
  WHERE
    workspace_id = dataops_prd.libs.get_workspace_id()
    AND COALESCE(source_table_catalog, target_table_catalog) NOT IN ('system', 'samples')
    AND COALESCE(source_table_full_name, target_table_full_name) IS NOT NULL
  QUALIFY rn = 1

  UNION

  SELECT
    request_params.full_name_arg AS fqn,
    event_time,
    ROW_NUMBER() OVER (
      PARTITION BY request_params.full_name_arg
      ORDER BY event_time DESC
    ) AS rn
  FROM system.access.audit
  WHERE
    request_params.workspace_id = dataops_prd.libs.get_workspace_id()
    AND (
      request_params.operation = 'READ'
      OR action_name IN (
        'createTable',
        'commandSubmit',
        'getTable',
        'deleteTable'
      )
    )
    AND response.status_code = 200
    AND request_params.full_name_arg IS NOT NULL
    AND request_params.full_name_arg NOT LIKE 'system%'
    AND request_params.full_name_arg NOT LIKE 'sample%'
  QUALIFY rn = 1
),
filtered AS (
  SELECT
    fqn,
    event_time,
    ROW_NUMBER() OVER (
      PARTITION BY fqn
      ORDER BY event_time DESC
    ) AS rnm
  FROM all_data
  WHERE fqn IN (
    SELECT DISTINCT
      CONCAT(table_catalog, '.', table_schema, '.', table_name)
    FROM system.information_schema.tables
    WHERE
      table_catalog NOT IN ('system', 'samples')
      AND table_schema NOT IN ('information_schema')
  )
  QUALIFY rnm = 1
)
SELECT DISTINCT
  fqn AS full_table_name,
  SPLIT(fqn, '\\.')[0] AS catalog_name,
  SPLIT(fqn, '\\.')[1] AS schema_name,
  SPLIT(fqn, '\\.')[2] AS tab_name,
  event_time AS last_access
FROM filtered