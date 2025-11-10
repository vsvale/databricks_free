CREATE OR REFRESH STREAMING TABLE ubereats.bronze.tb_mssql_users (
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw MSSQL users from JSON files. Bronze layer - preserves all data as-is with metadata tracking."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "mssql",
  "layer" = "raw_ingestion"
)
AS SELECT *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/ubereats/raw/datalake-ubereats/json-files/json-files/mssql_users_*',
  format => 'json'
);
