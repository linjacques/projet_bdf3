from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName("feeder - verif_jour.py") \
    .enableHiveSupport() \
    .getOrCreate()

source_file = "file:///app/bronze_data/temp/2025_06_17/parquet/part-00002-0647e098-05d6-4a02-ab34-82f776979550-c000.snappy.parquet"
target_dir = "file:///app/bronze_data/temp/2025_06_20/parquet/"


df = spark.read.parquet(source_file)
print(" Lecture du fichier source réussie")

df_updated = df.withColumn("situation_date", lit("2025-06-20"))

df_updated.write.mode("overwrite").parquet(target_dir)
print(f" Fichier sauvegardé dans {target_dir}")

spark.stop()
 