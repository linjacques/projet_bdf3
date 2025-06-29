import logging
from pyspark.sql import SparkSession
import os
from datetime import datetime

# 1. Logger
def setup_logger(log_dir="logs", log_file=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        log_file = f"gold_{timestamp}.log"
    log_path = os.path.join(log_dir, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler()]
    )
    logging.info(f"Logger initialisé, les logs seront enregistrés ici : {log_path}")

setup_logger()

# 2. Spark avec support Hive
try:
    spark = SparkSession.builder \
        .appName("datamart - datamarts.py") \
        .enableHiveSupport() \
        .config("spark.jars", "/opt/hive/lib/postgresql-9.4.1208.jre7.jar") \
        .getOrCreate()
    logging.info(" SparkSession initialisée avec support Hive.")
except Exception as e:
    logging.error(f" Erreur lors de l'initialisation de Spark : {e}")
    raise

# 3. Lecture depuis Hive
table_hive = "default.final_joined_accidents"
try:
    logging.info(f" Lecture de la table Hive : {table_hive}")
    df = spark.table(table_hive)
    logging.info(f" Table chargée avec {df.count()} lignes.")
    df.printSchema()
except Exception as e:
    logging.error(f" Erreur lors de la lecture de la table Hive : {e}")
    raise


#Datamart 1 - Accidents par ville
try:
    from pyspark.sql.functions import count, avg

    df_city = df.groupBy("City", "State").agg(
        count("*").alias("total_accidents"),
        avg("Severity").alias("avg_severity"),
        avg("Distance_km").alias("avg_distance"),
        avg("duration_minutes_accident").alias("avg_duration")
    )
    df_city.write.jdbc(
        url="jdbc:postgresql://host.docker.internal:5342/datamart",
        table="dm_accidents_by_city",
        mode="overwrite",
        properties={"user": "postgres", "password": "130902", "driver": "org.postgresql.Driver"}
    )
    logging.info(" Datamart 1 (accidents par ville) exporté.")
except Exception as e:
    logging.error(f" Erreur datamart 1 : {e}")

# Datamart 2 - Accidents et conditions météo
try:
    df_weather = df.groupBy("Weather_Condition").agg(
        count("*").alias("total_accidents"),
        avg("Severity").alias("avg_severity"),
        avg("Temperature(C)").alias("avg_temp"),
        avg("Precipitation(cm)").alias("avg_precip"),
        avg("Visibility_km").alias("avg_visibility")
    )
    df_weather.write.jdbc(
        url="jdbc:postgresql://host.docker.internal:5342/datamart",
        table="dm_weather_accidents",
        mode="overwrite",
        properties={"user": "postgres", "password": "130902", "driver": "org.postgresql.Driver"}
    )
    logging.info(" Datamart 2 (accidents et météo) exporté.")
except Exception as e:
    logging.error(f" Erreur datamart 2 : {e}")

# Datamart 3 - Infrastructures
try:
    from pyspark.sql.functions import lit
    from functools import reduce

    infra_cols = ["Amenity", "Bump", "Railway", "Stop", "Junction", "Traffic_Signal"]
    df_infra = []

    for col_name in infra_cols:
        sub_df = df.filter(df[col_name] == True).agg(
            count("*").alias("total_accidents"),
            avg("Severity").alias("avg_severity")
        ).withColumn("infrastructure", lit(col_name))
        df_infra.append(sub_df)

    df_result = reduce(lambda a, b: a.unionByName(b), df_infra)

    df_result.write.jdbc(
        url="jdbc:postgresql://host.docker.internal:5342/datamart",
        table="dm_accidents_by_infra",
        mode="overwrite",
        properties={"user": "postgres", "password": "130902", "driver": "org.postgresql.Driver"}
    )
    logging.info(" Datamart 3 (accidents par infrastructure) exporté.")
except Exception as e:
    logging.error(f" Erreur datamart 3 : {e}")

# Datamart 5 - Accidents graves et météo
try:
    df_graves = df.filter(df["Severity"] == 4)
    df_grave_weather = df_graves.groupBy("Weather_Condition").agg(
        count("*").alias("nb_grave_accidents"),
        avg("Temperature(C)").alias("avg_temp"),
        avg("Wind_Speed_kmh").alias("avg_wind"),
        avg("Humidity(%)").alias("avg_humidity")
    )
    df_grave_weather.write.jdbc(
        url="jdbc:postgresql://host.docker.internal:5342/datamart",
        table="dm_grave_accidents_weather",
        mode="overwrite",
        properties={"user": "postgres", "password": "130902", "driver": "org.postgresql.Driver"}
    )
    logging.info(" Datamart 5 (accidents graves et météo) exporté.")
except Exception as e:
    logging.error(f" Erreur datamart 5 : {e}")


# 5. Export vers PostgreSQL
try:
    df.write.jdbc(
        url="jdbc:postgresql://host.docker.internal:5342/datamart",
        table="gold_final_accidents",
        mode="overwrite",
        properties={
            "user": "postgres",
            "password": "130902",
            "driver": "org.postgresql.Driver"
        }
    )
    logging.info(" Données exportées avec succès vers PostgreSQL.")
except Exception as e:
    logging.error(f" Erreur export PostgreSQL : {e}")
    raise

# 6. Fin
spark.stop()
logging.info(" Traitement terminé.")
