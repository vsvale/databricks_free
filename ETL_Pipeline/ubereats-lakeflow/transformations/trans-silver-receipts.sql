CREATE OR REFRESH STREAMING TABLE ubereats.silver.tb_receipts_enriched
COMMENT "Cleaned receipts with quality checks, temporal enrichments, and business categorizations"
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "silver",
  "layer" = "cleaned_enriched"
)
AS SELECT *
FROM STREAM ubereats.bronze.tb_receipts;