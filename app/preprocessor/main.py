from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName("test-hive") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS test_hive")
spark.sql("USE metastore_hive")

df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
df = df.withColumn("date", lit("2025-06-26"))

df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .saveAsTable("metastore_hive.test_ctypes_resolu")

print("✅ Success")
