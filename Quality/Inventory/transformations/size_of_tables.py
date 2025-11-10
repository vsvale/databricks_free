# %pip install dbl-discoverx
# from pyspark import pipelines as dp

# small_file_max_size_MB = 10
# min_file_number = 100

# from discoverx import DX

# dx = DX()

# from pyspark.sql.functions import col, lit

# from_table_statement = spark.sql("""SELECT collect_list(concat(table_catalog,'.',table_schema,'.',table_name)) as fqn 
#                         FROM system.information_schema.tables
#                         WHERE
#                         table_catalog NOT IN ('system', 'samples')
#                         AND table_schema NOT IN ('information_schema')
#       """).first()[0]


# from_table_statement = ".".join([catalogs, schemas, tables])

# @dp.table
# def size_of_tables():
#     return dx.from_tables(from_table_statement).with_sql("DESCRIBE DETAIL {full_table_name}").apply().withColumn(
#     "average_file_size_MB", col("sizeInBytes") / col("numFiles") / 1024 / 1024
# ).withColumn(
#     "has_too_many_small_files",
#     (col("average_file_size_MB") < small_file_max_size_MB) & (col("numFiles") > min_file_number),
# ).filter(
#     "has_too_many_small_files"
# )