from pyspark.sql import SparkSession

# Création de la SparkSession
spark = SparkSession.builder \
    .appName("Write to HDFS") \
    .getOrCreate()

# Exemple de DataFrame
df = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob")
], ["id", "name"])

# Écriture en Parquet dans HDFS
df.write.mode("overwrite").parquet("hdfs://namenode:8020/user/hive/warehouse/test_table")

# Lecture pour vérification
df2 = spark.read.parquet("hdfs://namenode:8020/user/hive/warehouse/test_table")
df2.show()
