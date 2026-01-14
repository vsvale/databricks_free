-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS system_prd;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for schema_name in spark.sql("SELECT collect_set(schema_name)  FROM system.information_schema.schemata where catalog_name = 'system'").first()[0]:
-- MAGIC     spark.sql(f"CREATE SCHEMA IF NOT EXISTS system_prd.{schema_name}")
