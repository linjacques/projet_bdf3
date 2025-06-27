from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import os
import sys
import io
import logging
from datetime import datetime
from pyspark.sql.functions import round as spark_round,mean,percentile_approx, count, unix_timestamp, round, upper, trim, when, radians,isnan, sin, cos, col
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import FloatType, DoubleType
import logging
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
        logging.info(f" Colonnes supprimées après conversion : {', '.join(cols_to_drop)}")
    else:
        logging.info(" Aucune colonne à supprimer.")

    df = df.withColumn("duration_minutes_accident",
                       round((unix_timestamp("End_Time") - unix_timestamp("Start_Time")) / 60, 2))
    df = df.withColumn("duration_minutes_record_weather",
                       round((unix_timestamp("Weather_Timestamp") - unix_timestamp("Start_Time")) / 60, 2))
    df = df.drop("Start_Time", "End_Time", "Weather_Timestamp")

    return df

def transform_categorical_features(df):
    logging.info(" Début de la transformation des variables catégorielles...")

    df = df.fillna({
        "Timezone": "UNKNOWN",
        "State": "UNKNOWN",
        "Weather_Condition": "UNKNOWN"
    })

    indexers = [
        StringIndexer(inputCol="Timezone", outputCol="Timezone_index", handleInvalid="keep"),
        StringIndexer(inputCol="State", outputCol="State_index", handleInvalid="keep"),
        StringIndexer(inputCol="Weather_Condition", outputCol="Weather_Condition_index", handleInvalid="keep")
    ]

    pipeline_stages = indexers

    pipeline = Pipeline(stages=pipeline_stages)
    model = pipeline.fit(df)
    df_transformed = model.transform(df)
    df_transformed = df_transformed.drop("Timezone", "State", "Weather_Condition")

    logging.info(" Transformation des variables catégorielles terminée.")
    return df_transformed

def clean_wind_direction(df):
    df = df.withColumn("Wind_Direction_clean", upper(trim(col("Wind_Direction"))))

    df = df.withColumn("Wind_Direction_clean", when(col("Wind_Direction_clean").isin("CALM", "VARIABLE", "VAR", "NULL"), "UNKNOWN")
                       .when(col("Wind_Direction_clean") == "VAR", "VARIABLE")
                       .when(col("Wind_Direction_clean") == "EAST", "E")
                       .when(col("Wind_Direction_clean") == "WEST", "W")
                       .when(col("Wind_Direction_clean") == "NORTH", "N")
                       .when(col("Wind_Direction_clean") == "SOUTH", "S")
                       .otherwise(col("Wind_Direction_clean")))

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

    df = df.withColumn("wind_dir_sin", sin(radians(col("wind_angle"))))
    df = df.withColumn("wind_dir_cos", cos(radians(col("wind_angle"))))
    df = df.fillna({"wind_dir_sin": 0.0, "wind_dir_cos": 0.0})
    return df

def transform_binary_features(df):
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

def manage_null_values(df,
                       numeric_mean_cols=None,
                       numeric_zero_cols=None,
                       numeric_median_cols=None):
    logging.info("Début de la gestion des valeurs NULL...")

    # On initialise les listes si elles sont None
    numeric_mean_cols = numeric_mean_cols or []
    numeric_zero_cols = numeric_zero_cols or []
    numeric_median_cols = numeric_median_cols or []

    # Remplir par la moyenne
    for col_name in numeric_mean_cols:
        if col_name in df.columns:
            mean_val = df.select(mean(col(col_name))).first()[0]
            df = df.fillna({col_name: mean_val})
            df = df.withColumn(col_name, spark_round(col(col_name), 2))
        else:
            logging.warning(f"Colonne absente (mean) : {col_name}")

    # Remplir par zéro
    for col_name in numeric_zero_cols:
        if col_name in df.columns:
            df = df.fillna({col_name: 0.0})
            df = df.withColumn(col_name, spark_round(col(col_name), 2))
        else:
            logging.warning(f"Colonne absente (zéro) : {col_name}")

    # Remplir par médiane
    for col_name in numeric_median_cols:
        if col_name in df.columns:
            median_val = df.select(percentile_approx(col_name, 0.5)).first()[0]
            df = df.fillna({col_name: median_val})
            df = df.withColumn(col_name, spark_round(col(col_name), 2))
        else:
            logging.warning(f"Colonne absente (médiane) : {col_name}")

    logging.info(f"Fin de la gestion des valeurs NULL avec arrondi: {df}")
    return df


def drop_columns(df, columns_to_drop):
    existing_cols_to_drop = [col for col in columns_to_drop if col in df.columns]
    if not existing_cols_to_drop:
        logging.warning("Aucune colonne à supprimer, aucune des colonnes spécifiées n'existe dans le DataFrame.")
        return df

    df_dropped = df.drop(*existing_cols_to_drop)
    logging.info(f"Colonnes supprimées : {', '.join(existing_cols_to_drop)}")
    return df_dropped

def save_to_hive(df, table_name):
    logging.info(f"Sauvegarde dans la table Hive : {table_name}")
    df.coalesce(1).write.mode("overwrite").format("parquet").saveAsTable(table_name)
    logging.info(f"Données enregistrées dans Hive sous : {table_name}")

def inspect_last_bronze_traitement_and_save(spark):
    last_date = get_last_bronze_date(spark)
    if not last_date:
        logging.warning("Pas de dossier bronze à inspecter.")
        return

    logging.info(f" Inspection de la dernière date Bronze : {last_date}")
    path = Config.get_parquet_path(Config.BRONZE_ROOT, last_date)

    try:
        df = spark.read.parquet(path)
        df2 = spark.read.parquet(path)
        logging.info(f" Données chargées depuis : {path}")

        df = apply_unit_conversions(df)
        df2 = apply_unit_conversions(df2)

        df = clean_wind_direction(df)
        df2 = clean_wind_direction(df2)

        df = transform_categorical_features(df)


        colonnes_a_supprimer = ["Description", "situation_date","Airport_Code", "Source", "Street","City","Country", "County", "Wind_Direction", "wind_angle", "Wind_Direction_clean"]
        df = drop_columns(df, colonnes_a_supprimer)
        colonnes_a_supprimer2 = ["situation_date", "Source", "Country", "Wind_Direction", "wind_angle","County", "wind_dir_sin" , "wind_dir_cos"]
        df2 = drop_columns(df2, colonnes_a_supprimer2)


        df = transform_binary_features(df)

        cols_to_drop = [
            "Start_Lat", "Start_Lng", "End_Lat", "End_Lng",
            "Start_Time", "End_Time", "Zipcode"
        ]
        df = df.drop(*cols_to_drop)

        df = manage_null_values(
            df,
            numeric_mean_cols=["Temperature(C)", "Humidity(%)", "Pressure(in)", "Wind_Speed_kmh", "Visibility_km"],
            numeric_zero_cols=["Precipitation(cm)", "duration_minutes_record_weather"],
            numeric_median_cols=["Wind_Chill(C)"]
        )

        df2 = manage_null_values(
            df2,
            numeric_mean_cols=["Temperature(C)", "Humidity(%)", "Pressure(in)", "Wind_Speed_kmh", "Visibility_km"],
            numeric_zero_cols=["Precipitation(cm)", "duration_minutes_record_weather"],
            numeric_median_cols=["Wind_Chill(C)"]
        )

        logging.info("--- Aperçu des 5 premières lignes ---")
        log_df_show(df, 100)
        log_df_show(df2, 100)

        logging.info("--- Schéma des colonnes ---")
        schema_str = df._jdf.schema().treeString()
        logging.info(schema_str)
        schema_str2 = df2._jdf.schema().treeString()
        logging.info(schema_str2)

        logging.info("--- Liste complète des colonnes ---")
        logging.info(", ".join(df.columns))


        # Étape 1 : suppression des lignes avec null/NaN dans les colonnes twilight
        twilight_columns = ["Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]
        df2_cleaned = df2.dropna(subset=twilight_columns)


        # 1. Remplacer par 'other'
        cols_to_replace_other = ["Zipcode", "Timezone", "Airport_Code", "Street", "City"]
        for c in cols_to_replace_other:
            df2_cleaned = df2_cleaned.withColumn(
                c,
                when(col(c).isNull() | (col(c) == ""), "Other").otherwise(col(c))
            )

        # 2. Remplacer Description par 'no description'
        df2_cleaned = df2_cleaned.withColumn(
            "Description",
            when(col("Description").isNull() | (col("Description") == ""), "No_description").otherwise(col("Description"))
        )

        # 3. Remplacer Weather_Condition par 'Other'
        df2_cleaned = df2_cleaned.withColumn(
            "Weather_Condition",
            when(col("Weather_Condition").isNull() | (col("Weather_Condition") == ""), "Other").otherwise(col("Weather_Condition"))
        )

        # Tu peux maintenant voir les changements
        df2_cleaned.show(truncate=False)

        total_count = df.count()
        logging.info(f"--- Nombre total de lignes : {total_count}")
        null_counts = df.select([
            count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
            for c in df.columns
        ]) 

        null_counts.show(truncate=False)

        # Récupération des types des colonnes
        schema = dict(df2_cleaned.dtypes)  # {colname: dtype}

        # Construction des expressions conditionnelles
        null_counts_expr = []

        for c in df2_cleaned.columns:
            dtype = schema[c]
            if dtype in ["float", "double"]:
                condition = col(c).isNull() | isnan(col(c))
            else:
                condition = col(c).isNull()

            null_counts_expr.append(count(when(condition, c)).alias(c))

        # Application du select avec les expressions
        null_counts2 = df2_cleaned.select(null_counts_expr)
        null_counts2.show(truncate=False)

        # save_to_hive(df, "default.preprocessed_accidents_for_ml")
        save_to_hive(df2_cleaned, "default.preprocessed_accidents_for_join")


    except Exception as e:
        logging.error(f"Erreur pendant l'inspection du Bronze : {e}")

setup_logger()

spark = SparkSession.builder \
    .appName("Silver - pré-traitement") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

logging.info("Spark session initialisée")

inspect_last_bronze_traitement_and_save(spark)

spark.stop()
logging.info("Fin de l analyse pré-traitement")
