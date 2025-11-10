CREATE MATERIALIZED VIEW IF NOT EXISTS quality.default.audit_hist
CLUSTER BY AUTO
COMMENT 'audit hist from system.access.audit'
TRIGGER ON UPDATE
TBLPROPERTIES(pipelines.channel = "PREVIEW")
AS
SELECT * from system.access.audit WHERE event_date BETWEEN date_sub(current_date(), 1) AND current_date()