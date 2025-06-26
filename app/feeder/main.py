from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from config import Config

import logging
import os
from datetime import datetime

def setup_logger(log_dir="logs", log_file=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if not log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        log_file = f"feeder_{timestamp}.log"

    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )

    logging.info(f"Logger initialisé, les logs seront enregistrés ici : {log_path}")


setup_logger()


spark = SparkSession.builder \
    .appName("Feeder Optimisé Incrémental") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

logging.info(" Spark prêt")


def get_last_bronze_date():
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(Config.BRONZE_ROOT)
        if not fs.exists(path):
            return None
        statuses = fs.listStatus(path)
        dates = [status.getPath().getName() for status in statuses if status.isDirectory()]
        return sorted(dates)[-1] if dates else None
    except Py4JJavaError as e:
        print(f"Erreur en listant les dossiers bronze HDFS : {e}")
        return None

# Étape 1 : identifier la dernière date Bronze cumulée
last_bronze_date = get_last_bronze_date()
logging.info(f" Dernier bronze trouvé : {last_bronze_date}")

# Étape 2 : lister les dates Temp disponibles (local)
all_temp_dates = Config.get_temp_dates(spark)
logging.info(f" Dates temp disponibles : {all_temp_dates}")

# Étape 3 : déterminer les dates à traiter
dates_to_process = [d for d in all_temp_dates if (last_bronze_date is None or d > last_bronze_date)]
logging.info(f" Nouvelles dates à traiter : {dates_to_process}")

# Étape 4 : boucle de traitement incrémental
for new_date in dates_to_process:
    logging.info(f"\n Traitement du jour : {new_date}")

    path_today = Config.get_parquet_path(Config.TEMP_PATH, new_date)


    df_today = spark.read.parquet(path_today)
    logging.info(f" {df_today.count()} lignes lues depuis {path_today}")

    if last_bronze_date:
        path_bronze = Config.get_parquet_path(Config.BRONZE_ROOT, last_bronze_date)
        # Par défaut ici, path_bronze = "hdfs://namenode:8020/save_hdfs/union/{last_bronze_date}/parquet"
        path_bronze = path_bronze if path_bronze.endswith("parquet") else f"{path_bronze}/parquet"
        try:
            df_prev = spark.read.parquet(path_bronze)
            logging.info(f" {df_prev.count()} lignes dans le bronze précédent : {last_bronze_date}")
            accumulated = df_prev.union(df_today)
        except Exception as e:
            logging.warning(f" Bronze précédent non trouvé ou erreur lecture : {e}")
            logging.info(" Initialisation Bronze avec la date courante uniquement.")
            accumulated = df_today
    else:
        accumulated = df_today

    if "State" in accumulated.columns:
        accumulated = accumulated.repartition(10, "State")
    else:
        accumulated = accumulated.repartition(10)

    new_bronze_path = Config.get_parquet_path(Config.BRONZE_ROOT, new_date)
    logging.info(f" Sauvegarde Bronze dans HDFS : {new_bronze_path}")
    accumulated.coalesce(1).write.mode("overwrite").parquet(new_bronze_path)

    logging.info(f" Bronze mis à jour pour {new_date} : {accumulated.count()} lignes")

    last_bronze_date = new_date


spark.stop()
logging.info(" Traitement terminé")