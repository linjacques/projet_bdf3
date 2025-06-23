import os
import logging
import traceback
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

class Config:
    TEMP_PATH = "/app/bronze_data/temp"
    BRONZE_ROOT = "hdfs://namenode:8020/save_hdfs/pre_bronze"

    @staticmethod
    def get_temp_dates():
        try:
            return sorted(os.listdir(Config.TEMP_PATH))
        except FileNotFoundError:
            logging.error(f"Dossier temp non trouvé : {Config.TEMP_PATH}")
            return []

    @staticmethod
    def get_parquet_path(base, date_str):
        return os.path.join(base, date_str, "parquet")

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Feeder Local vers HDFS") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

logging.info(" Spark initialisé")

# Lire les dates disponibles en local
dates_available = Config.get_temp_dates()
logging.info(f" Dates détectées dans temp : {dates_available}")

# Boucle sur chaque date à traiter
for date_str in dates_available:
    try:
        logging.info(f"\n Traitement de la date : {date_str}")

        # 1. Lire le fichier parquet local
        local_path = f"file://{Config.get_parquet_path(Config.TEMP_PATH, date_str)}"
        logging.info(f" Lecture depuis : {local_path}")

        df = spark.read.parquet(local_path)
        logging.info(f" {df.count()} lignes lues pour la date {date_str}")

        # 2. Déterminer le chemin HDFS cible
        bronze_path = Config.get_parquet_path(Config.BRONZE_ROOT, date_str)
        logging.info(f" Écriture prévue vers : {bronze_path}")

        # 3. Supprimer l'ancien dossier HDFS s'il existe
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(bronze_path)
        if fs.exists(hdfs_path):
            logging.warning(f" Le dossier HDFS existe déjà : suppression de {bronze_path}")
            fs.delete(hdfs_path, True)

        # 4. Écriture dans HDFS sans coalesce (laisser Spark gérer les partitions)
        df.write.mode("overwrite").parquet(bronze_path)
        logging.info(f" Données écrites dans HDFS pour {date_str} ({df.count()} lignes)")

    except Py4JJavaError as e:
        logging.error(f" Erreur Java Spark pour la date {date_str} : {e}")
        traceback.print_exc()
    except Exception as e:
        logging.error(f" Erreur inattendue pour la date {date_str} : {e}")
        traceback.print_exc()

spark.stop()
logging.info(" Traitement terminé")
