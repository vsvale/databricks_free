-- ============================================================================
-- BRONZE LAYER: Raw Orders Ingestion
-- ============================================================================
-- Purpose: Ingest raw Kafka order events with Auto Loader
-- Source: JSON files from Volumes
-- Demo: Shows streaming ingestion with basic quality tracking
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE ubereats.bronze.tb_orders(
  CONSTRAINT no_rescued_data
  EXPECT (_rescued_data IS NULL)
)
COMMENT "Raw Kafka orders from JSON files. Bronze layer - preserves all data as-is with metadata tracking."
TBLPROPERTIES (
  "quality" = "bronze",
  "source_system" = "kafka",
  "layer" = "raw_ingestion"
)
AS SELECT *,
  -- Raw order fields (preserved as-is)
 -- Auto Loader metadata for lineage tracking
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  '/Volumes/ubereats/raw/datalake-ubereats/json-files/json-files/kafka_orders_*',
  format => 'json'
);