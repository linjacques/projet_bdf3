from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
print("✅ Hello from Spark!")
spark.stop()
