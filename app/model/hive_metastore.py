import os
import psycopg2
from pyspark.sql import SparkSession

# Étape 2 : Créer les tables Hive avec Spark
spark = SparkSession.builder \
    .appName("Creation du Hive metastore") \
    .enableHiveSupport() \
    .getOrCreate()

# Tables externes : bronze, silver, gold
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS metastore_hive.bronze (
    id INT,
    nom STRING,
    email STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lakehouse/bronze'
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS metastore_hive.silver (
    id INT,
    nom STRING,
    email STRING,
    email_valide BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lakehouse/silver'
""")

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS metastore_hive.gold (
    id INT,
    nom STRING,
    domaine_email STRING,
    pays STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/lakehouse/gold'
""")

print("✅ Tables Hive créées.")
