# Databricks notebook source
# MAGIC %md
# MAGIC # Para cada catalogo, schema e tabela faca um describe details

# COMMAND ----------

# MAGIC %sql
# MAGIC     SELECT DISTINCT
# MAGIC       CONCAT(table_catalog, '.', table_schema, '.', table_name)
# MAGIC     FROM system.information_schema.tables
# MAGIC     WHERE
# MAGIC       table_catalog NOT IN ('system', 'samples')
# MAGIC       AND table_schema NOT IN ('information_schema')
