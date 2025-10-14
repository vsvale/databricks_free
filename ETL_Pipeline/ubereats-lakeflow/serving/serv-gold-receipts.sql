CREATE MATERIALIZED VIEW ubereats.gold.tb_receipts 
COMMENT "Daily receipts metrics. Ready for BI dashboards."
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "business_analytics"
)
AS
SELECT
    *
FROM ubereats.silver.tb_receipts_enriched;