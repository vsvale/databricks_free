CREATE OR REFRESH STREAMING TABLE ubereats.silver.tb_payments_enriched
COMMENT "Cleaned payments with quality checks, temporal enrichments, and business categorizations"
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "silver",
  "layer" = "cleaned_enriched"
)
AS SELECT *
FROM STREAM ubereats.bronze.tb_payments;