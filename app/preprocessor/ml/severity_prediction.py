from pyspark.sql import SparkSession
from pyspark.sql.types import NumericType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
from datetime import datetime
import logging
import os
import time

def setup_logger(log_dir="logs", log_file=None):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if not log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        log_file = f"severity_prediction_{timestamp}.log"

    log_path = os.path.join(log_dir, log_file)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logger initialized. Logs will be saved to: {log_path}")

# -------------------- MAIN PIPELINE --------------------

setup_logger()

try:
    start_time = time.time()

    logging.info(" Initialisation SparkSession...")
    spark = SparkSession.builder \
        .appName("AccidentSeverityPrediction") \
        .enableHiveSupport() \
        .getOrCreate()

    logging.info(" Lecture de la table Hive : default.preprocessed_accidents_for_ml")
    df = spark.table("default.preprocessed_accidents_for_ml")

    logging.info(" Sous-échantillonnage (10 %) pour éviter surcharge mémoire...")
    df = df.sample(fraction=0.1, seed=42)  # Retire cette ligne en prod

    target_col = "Severity"
    numeric_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, NumericType) and f.name != target_col
    ]

    logging.info(" Suppression des lignes avec valeurs nulles...")
    df_clean = df.na.drop(subset=numeric_cols + [target_col])

    logging.info(f" Assemblage des features ({len(numeric_cols)} colonnes numériques)...")
    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    df_with_features = assembler.transform(df_clean).persist()

    logging.info(" Split (80/20)...")
    train_data, test_data = df_with_features.randomSplit([0.8, 0.2], seed=42)

    logging.info(" Entraînement du modèle RandomForestClassifier...")
    rf = RandomForestClassifier(
        labelCol=target_col,
        featuresCol="features",
        numTrees=50,
        maxBins=256
    )
    model = rf.fit(train_data)

    logging.info(" Prédiction sur toutes les données...")
    full_predictions = model.transform(df_with_features)

    logging.info(" Ajout de la colonne prediction_classe...")
    full_predictions = full_predictions.withColumn(
        "prediction_classe", col("prediction").cast("int")
    )

    original_cols = df.columns
    df_final = full_predictions.select(*original_cols, "prediction_classe")

    logging.info(" Évaluation de l’accuracy sur données test...")
    evaluator = MulticlassClassificationEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(model.transform(test_data))
    logging.info(f" Accuracy du modèle Random Forest : {accuracy:.4f}")

    logging.info(" Aperçu du résultat final (100 lignes max) :")
    sample_rows = df_final.limit(100).collect()
    header = "\t".join(df_final.columns)
    logging.info(header)
    for row in sample_rows:
        row_str = "\t".join([str(item) if item is not None else "" for item in row])
        logging.info(row_str)

    logging.info(" Sauvegarde Hive : default.predicted_severity")
    df_final.write.mode("overwrite").saveAsTable("default.predicted_severity")

    total_time = round(time.time() - start_time, 2)
    logging.info(f" Temps total d'exécution : {total_time} secondes")

except Exception as e:
    logging.exception(" Une erreur est survenue pendant l'exécution du script.")
finally:
    logging.info(" Arrêt de la SparkSession.")
    spark.stop()
