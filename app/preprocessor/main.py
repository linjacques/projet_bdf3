from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lakehouse Pipeline") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-server:9083") \
    .enableHiveSupport() \
    .getOrCreate()

fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
Path = spark._jvm.org.apache.hadoop.fs.Path

for layer in ["bronze", "silver", "gold"]:
    path = Path(f"hdfs://namenode:8020/lakehouse/{layer}")
    if not fs.exists(path):
        fs.mkdirs(path)

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5342/metastore_hive") \
    .option("user", "postgres") \
    .option("password", "130902") \
    .option("dbtable", '"TBLS"') \
    .option("driver", "org.postgresql.Driver")\
    .load()

df.show()
