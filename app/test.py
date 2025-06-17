from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-ui").getOrCreate()

df = spark.range(0, 1000000)
df.selectExpr("id * 2 as double_id").show()

input("Press Enter to stop the job...")  # Garde Spark actif
