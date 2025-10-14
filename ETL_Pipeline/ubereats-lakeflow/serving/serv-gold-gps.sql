CREATE MATERIALIZED VIEW ubereats.gold.tb_gps 
COMMENT "Daily gps metrics. Ready for BI dashboards."
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "business_analytics"
)
AS
SELECT
    *
FROM ubereats.silver.tb_gps_enriched;