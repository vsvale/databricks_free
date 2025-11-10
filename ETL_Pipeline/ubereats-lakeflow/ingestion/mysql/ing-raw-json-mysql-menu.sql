CREATE OR REFRESH STREAMING TABLE ubereats.bronze.tb_menu (
  CONSTRAINT no_rescued_data EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw MySQL menu data from JSON files. Bronze layer - preserves all data as-is with metadata tracking."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "mysql",
  "layer" = "raw_ingestion"
)
AS SELECT *,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/ubereats/raw/datalake-ubereats/json-files/json-files/mysql_menu_*',
  format => 'json'
);
