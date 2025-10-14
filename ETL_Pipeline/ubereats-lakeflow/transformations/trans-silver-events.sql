CREATE OR REFRESH STREAMING TABLE ubereats.silver.tb_events_enriched
COMMENT "Cleaned events with quality checks, temporal enrichments, and business categorizations"
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "silver",
  "layer" = "cleaned_enriched"
)
AS SELECT *
FROM STREAM ubereats.bronze.tb_events;