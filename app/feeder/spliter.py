from pyspark.sql import SparkSession, functions as F
import os

# Configuration simple
class Config:
    SIMULATION_DATES = ["2025-06-15", "2025-06-16", "2025-06-17"]
    BRONZE_PATH = "./bronze_data"

class DataSplitter:

    def __init__(self, spark):
        self.spark = spark

    def split_data_into_parts(self, df):
        print(" Splitting data into 3 equal parts")
        df1, df2, df3 = df.randomSplit([0.33, 0.33, 0.34], seed=42)
        counts = [df1.count(), df2.count(), df3.count()]
        print(f" Data split done: Part1={counts[0]}, Part2={counts[1]}, Part3={counts[2]}")
        return df1, df2, df3

    def add_situation_date_to_part(self, df_part, simulation_date):
        print(f" Adding situation_date '{simulation_date}' to data part")
        return df_part.withColumn("situation_date", F.lit(simulation_date))

    def save_individual_parts(self, df_parts):
        print(" Saving parts with situation_date")

        saved_parts = {}

        for i, (df_part, date_str) in enumerate(zip(df_parts, Config.SIMULATION_DATES)):
            print(f"--- Processing part {i+1} for date {date_str} ---")

            df_with_date = self.add_situation_date_to_part(df_part, date_str)

            # Optimisation repartition
            if "State" in df_with_date.columns:
                df_optimized = df_with_date.repartition(5, "State").cache()
            else:
                df_optimized = df_with_date.repartition(5).cache()

            # Création des chemins locaux
            safe_date = date_str.replace("-", "_")
            parquet_path = os.path.join(Config.BRONZE_PATH, "temp", safe_date, "parquet")
            csv_path = os.path.join(Config.BRONZE_PATH, "temp", safe_date, "csv")

            # Sauvegarde Parquet
            df_optimized.write.mode("overwrite").parquet(parquet_path)

            # Sauvegarde CSV (avec header)
            df_optimized.write.mode("overwrite").option("header", True).csv(csv_path)

            count = df_optimized.count()
            print(f"➡ Part {i+1} contient {count} lignes")
            print(f"    → Parquet saved at: {parquet_path}")
            print(f"    → CSV saved at: {csv_path}")

            saved_parts[date_str] = df_optimized

        return saved_parts


if __name__ == "__main__":
    # Initialisation Spark en local
    spark = SparkSession.builder \
        .appName("Split DataFrame") \
        .master("local[*]") \
        .getOrCreate()

    # Chargement CSV local (à adapter selon ton chemin)
    df_initial = spark.read.csv("file:///app/source/US_Accidents_March23.csv", header=True, inferSchema=True)

    splitter = DataSplitter(spark)
    parts = splitter.split_data_into_parts(df_initial)
    splitter.save_individual_parts(parts)

    spark.stop()
 