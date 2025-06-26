from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import os
import sys
import io
import logging
from datetime import datetime
from pyspark.sql.functions import round as spark_round, count, unix_timestamp, round, upper, trim, when, radians,isnan, sin, cos, col
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline


# Ajout du dossier parent dans le sys.path pour importer feeder.config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from feeder.config import Config

def setup_logger(log_dir="logs", log_file=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if not log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        log_file = f"pre_traitement_{timestamp}.log"

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

def get_last_bronze_date(spark):
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(Config.BRONZE_ROOT)

        if not fs.exists(path):
            logging.warning(f"Aucun dossier trouvé dans : {Config.BRONZE_ROOT}")
            return None

        statuses = fs.listStatus(path)
        dates = [status.getPath().getName() for status in statuses if status.isDirectory()]
        return sorted(dates)[-1] if dates else None

    except Py4JJavaError as e:
        logging.error(f"Erreur HDFS lors de la récupération de la dernière date Bronze : {e}")
        return None

def log_df_show(df, n=5):
    buffer = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buffer
    try:
        df.show(n, truncate=False)
    finally:
        sys.stdout = old_stdout
    logging.info(buffer.getvalue())

def apply_unit_conversions(df):
    conversions = {
        "Distance(mi)": ("Distance_km", col("Distance(mi)") * 1.60934),
        "Temperature(F)": ("Temperature(C)", (col("Temperature(F)") - 32) * 5.0 / 9.0),
        "Wind_Chill(F)": ("Wind_Chill(C)", (col("Wind_Chill(F)") - 32) * 5.0 / 9.0),
        "Visibility(mi)": ("Visibility_km", col("Visibility(mi)") * 1.60934),
        "Wind_Speed(mph)": ("Wind_Speed_kmh", col("Wind_Speed(mph)") * 1.60934),
        "Precipitation(in)": ("Precipitation(cm)", col("Precipitation(in)") * 2.54)
    }

    converted_cols = []

    for original_col, (new_col, expr) in conversions.items():
        if original_col in df.columns:
            df = df.withColumn(new_col, spark_round(expr, 2))
            converted_cols.append(new_col)
            logging.info(f" Conversion + arrondi appliqués : {original_col} → {new_col}")
        else:
            logging.warning(f" Colonne absente : {original_col} — conversion ignorée.")

    cols_to_drop = [col_name for col_name in conversions if col_name in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
        logging.info(f"🧹 Colonnes supprimées après conversion : {', '.join(cols_to_drop)}")
    else:
        logging.info(" Aucune colonne à supprimer.")

    df = df.withColumn("duration_minutes_accident",
                   round((unix_timestamp("End_Time") - unix_timestamp("Start_Time")) / 60, 2))
    df = df.withColumn("duration_minutes_record_weather",
                       round((unix_timestamp("Weather_Timestamp") - unix_timestamp("Start_Time")) / 60, 2))
    df = df.drop("Start_Time", "End_Time", "Weather_Timestamp")

    return df

def transform_categorical_features(df):
    logging.info("🔄 Début de la transformation des variables catégorielles...")

    # Étape 1 : remplacement des NULL
    df = df.fillna({
        "Timezone": "UNKNOWN",
        "State": "UNKNOWN",
        "Weather_Condition": "UNKNOWN"
    })

    # Étape 2 : définition des indexers
    indexers = [
        StringIndexer(inputCol="Timezone", outputCol="Timezone_index", handleInvalid="keep"),
        StringIndexer(inputCol="State", outputCol="State_index", handleInvalid="keep"),
        StringIndexer(inputCol="Weather_Condition", outputCol="Weather_Condition_index", handleInvalid="keep")
    ]

    pipeline_stages = indexers

    # Pipeline
    pipeline = Pipeline(stages=pipeline_stages)
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    df_transformed = df_transformed.drop("Timezone", "State", "Weather_Condition")

    logging.info("✅ Transformation des variables catégorielles terminée.")
    return df_transformed


def clean_wind_direction(df):
    df = df.withColumn("Wind_Direction_clean", upper(trim(col("Wind_Direction"))))

    # Harmonisation de quelques cas
    df = df.withColumn("Wind_Direction_clean", when(col("Wind_Direction_clean").isin("CALM", "VARIABLE", "VAR", "NULL"), "UNKNOWN")
                       .when(col("Wind_Direction_clean") == "VAR", "VARIABLE")
                       .when(col("Wind_Direction_clean") == "EAST", "E")
                       .when(col("Wind_Direction_clean") == "WEST", "W")
                       .when(col("Wind_Direction_clean") == "NORTH", "N")
                       .when(col("Wind_Direction_clean") == "SOUTH", "S")
                       .otherwise(col("Wind_Direction_clean")))

    # Étape 3 : Mapping direction ➝ angle
    df = df.withColumn("wind_angle",
                       when(col("Wind_Direction_clean") == "N", 0.0)
                       .when(col("Wind_Direction_clean") == "NNE", 22.5)
                       .when(col("Wind_Direction_clean") == "NE", 45.0)
                       .when(col("Wind_Direction_clean") == "ENE", 67.5)
                       .when(col("Wind_Direction_clean") == "E", 90.0)
                       .when(col("Wind_Direction_clean") == "ESE", 112.5)
                       .when(col("Wind_Direction_clean") == "SE", 135.0)
                       .when(col("Wind_Direction_clean") == "SSE", 157.5)
                       .when(col("Wind_Direction_clean") == "S", 180.0)
                       .when(col("Wind_Direction_clean") == "SSW", 202.5)
                       .when(col("Wind_Direction_clean") == "SW", 225.0)
                       .when(col("Wind_Direction_clean") == "WSW", 247.5)
                       .when(col("Wind_Direction_clean") == "W", 270.0)
                       .when(col("Wind_Direction_clean") == "WNW", 292.5)
                       .when(col("Wind_Direction_clean") == "NW", 315.0)
                       .when(col("Wind_Direction_clean") == "NNW", 337.5)
                       .otherwise(None)  # UNKNOWN, NULL ou autres
                       )

    # Étape 4 : Calcul de sin/cos directement dans Spark
    df = df.withColumn("wind_dir_sin", sin(radians(col("wind_angle"))))
    df = df.withColumn("wind_dir_cos", cos(radians(col("wind_angle"))))
    df = df.fillna({"wind_dir_sin": 0.0, "wind_dir_cos": 0.0})
    df = df.drop("Wind_Direction", "wind_angle", "Wind_Direction_clean")

    return df


def transform_binary_features(df):
    # Colonnes booléennes à transformer en 0/1
    bool_cols = [
        "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
        "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
        "Traffic_Signal", "Turning_Loop"
    ]

    for col_name in bool_cols:
        df = df.withColumn(
            col_name + "_num",
            when(col(col_name) == True, 1).otherwise(0)
        )
    df = df.drop(*bool_cols)

    # Colonnes day/night à transformer en 1/0 (day=1, night=0), null ou autre = -1
    day_night_cols = [
        "Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"
    ]

    for col_name in day_night_cols:
        df = df.withColumn(
            col_name + "_num",
            when(col(col_name).isin(['day', 'Day']), 1)
            .when(col(col_name).isin(['night', 'Night']), 0)
            .otherwise(-1)
        )
    df = df.drop(*day_night_cols)

    return df

def drop_columns(df, columns_to_drop):
    existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
    if not existing_cols_to_drop:
        logging.warning("Aucune colonne à supprimer, aucune des colonnes spécifiées n'existe dans le DataFrame.")
        return df

    df_dropped = df.drop(*existing_cols_to_drop)
    logging.info(f"Colonnes supprimées : {', '.join(existing_cols_to_drop)}")
    return df_dropped

def inspect_last_bronze(spark):
    last_date = get_last_bronze_date(spark)
    if not last_date:
        logging.warning("Pas de dossier bronze à inspecter.")
        return

    logging.info(f" Inspection de la dernière date Bronze : {last_date}")
    path = Config.get_parquet_path(Config.BRONZE_ROOT, last_date)

    try:
        df = spark.read.parquet(path)
        logging.info(f" Données chargées depuis : {path}")
        a=df.select("Wind_Direction").distinct().show(10000)
        print(a)

        df = apply_unit_conversions(df)
        df = clean_wind_direction(df)
        df = transform_categorical_features(df)
        colonnes_a_supprimer = ["ID", "Description", "situation_date"]
        df = drop_columns(df, colonnes_a_supprimer)
        df = transform_binary_features(df)

        logging.info("--- Aperçu des 5 premières lignes ---")
        log_df_show(df, 100)

        logging.info("--- Schéma des colonnes ---")
        schema_str = df._jdf.schema().treeString()
        logging.info(schema_str)

        logging.info("--- Statistiques descriptives sur colonnes numériques ---")
        numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ['IntegerType', 'DoubleType', 'LongType', 'FloatType']]
        if numeric_cols:
            summary_df = df.select(numeric_cols).summary()
            pandas_summary = summary_df.toPandas()
            logging.info(f"\n{pandas_summary.to_string(index=False)}")
        else:
            logging.info("Aucune colonne numérique détectée pour le résumé statistique.")

        logging.info("--- Liste complète des colonnes ---")
        logging.info(", ".join(df.columns))

        total_count = df.count()
        logging.info(f"--- Nombre total de lignes : {total_count}")
        null_counts = df.select([
            count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
            for c in df.columns
        ])

        null_counts.show(truncate=False)


    except Exception as e:
        logging.error(f"Erreur pendant l'inspection du Bronze : {e}")


# MAIN
setup_logger()

spark = SparkSession.builder \
    .appName("Inspection Bronze") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

logging.info("Spark session initialisée")

inspect_last_bronze(spark)

spark.stop()
logging.info("✅ Fin de l’analyse pré-traitement")
