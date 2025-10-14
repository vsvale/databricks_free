CREATE MATERIALIZED VIEW ubereats.gold.tb_payments 
COMMENT "Daily payments metrics. Ready for BI dashboards."
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "business_analytics"
)
AS
SELECT
    *
FROM ubereats.silver.tb_payments_enriched;