from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test JDBC") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5342/metastore_hive") \
    .option("user", "postgres") \
    .option("password", "130902") \
    .option("dbtable", '"VERSION"') \
    .load()

df.show()
