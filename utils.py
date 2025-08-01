



def create_schema(catalog_name, schema_name, spark=spark):
    catalog_name = catalog_name.replace("-", "_")
    schema_name = schema_name.replace("-", "_")
    try:
      spark.sql(f"USE SCHEMA {catalog_name}.{schema_name}")
    except Exception as e:
        create_catalog(catalog_name)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(f"USE SCHEMA {schema_name}")