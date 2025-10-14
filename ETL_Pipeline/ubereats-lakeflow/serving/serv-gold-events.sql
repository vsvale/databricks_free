CREATE MATERIALIZED VIEW ubereats.gold.tb_events 
COMMENT "Daily events metrics. Ready for BI dashboards."
CLUSTER BY AUTO
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "business_analytics"
)
AS
SELECT
    *
FROM ubereats.silver.tb_events_enriched;