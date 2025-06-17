from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Test PySpark") \
    .getOrCreate()

# Créer un DataFrame en mémoire
data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 22),
    ("Diana", 35)
]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# Transformation : filtrer les personnes âgées de plus de 25 ans
df_filtered = df.filter(col("age") > 25)

# Ajouter une colonne avec l'âge doublé
df_transformed = df_filtered.withColumn("double_age", col("age") * 2)

# Afficher le résultat
df_transformed.show()

# Fermer la session Spark
spark.stop()
