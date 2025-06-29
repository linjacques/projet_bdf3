import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Chemin vers les fichiers de config si nécessaire
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from feeder.config import Config

def setup_logger(log_dir="logs", log_file=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if not log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        log_file = f"join{timestamp}.log"

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

def save_to_hive(df, table_name):
    logging.info(f"Sauvegarde dans la table Hive : {table_name}")
    df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(table_name)
    logging.info(f"Données enregistrées dans Hive sous : {table_name}")

# Initialisation du logger
setup_logger()

try:
    # Création de la SparkSession avec Hive
    spark = SparkSession.builder \
        .appName("preprocessor - join_ml.py") \
        .enableHiveSupport() \
        .getOrCreate()
    logging.info("SparkSession initialisée avec support Hive.")

    # Lecture des deux tables Hive
    logging.info("Lecture de la table 'predicted_severity'...")
    df_ml = spark.table("default.predicted_severity_full")
    logging.info(f"Table df_ml chargée avec {df_ml.count()} lignes.")
    df_ml.printSchema()

    logging.info("Lecture de la table 'preprocessed_accidents_for_join'...")
    df_join = spark.table("default.preprocessed_accidents_for_join")
    logging.info(f"Table df_join chargée avec {df_join.count()} lignes.")
    df_join.printSchema()

    # Vérification de la présence de la colonne ID
    if "ID" not in df_ml.columns or "ID" not in df_join.columns:
        raise ValueError("La colonne 'ID' est absente dans l'une des tables.")

    # Sélection des colonnes utiles dans df_ml
    df_ml_selected = df_ml.select("ID", "prediction_classe")
    logging.info("Colonnes sélectionnées dans df_ml : ID et prediction_classe.")

    # Jointure : df_join (complet) + prediction_classe
    logging.info("Jointure des deux DataFrames sur 'ID' (inner join)...")
    df_final = df_join.join(df_ml_selected, on="ID", how="inner")

    final_count = df_final.count()
    logging.info(f"Jointure effectuée avec succès. Nombre de lignes finales : {final_count}")

    # Aperçu de quelques résultats
    logging.info("Aperçu des premières lignes après jointure :")
    for row in df_final.limit(10).collect():
        logging.info(row)

    # Sauvegarde dans Hive
    table_name = "default.final_joined_accidents"
    logging.info(f"Enregistrement du résultat dans la table Hive : {table_name}")
    save_to_hive(df_final, table_name)
    logging.info("Table sauvegardée avec succès.")

except Exception as e:
    logging.error(f"Une erreur est survenue : {str(e)}", exc_info=True)
finally:
    spark.stop()
    logging.info("SparkSession arrêtée.")
