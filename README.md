#  Projet Big Data Framework (BDF3)

##  À propos du projet

Ce projet Big Data a pour objectif de mettre en place une architecture complète de traitement de données dans un contexte proche du réel, depuis l’ingestion jusqu’à l’exploitation des données via des analyses et modèles de machine learning, le tout déployé dans un cluster Hadoop avec Spark sur YARN.

## Architecture générale

Le projet est structuré autour d’une architecture Lambda, avec :

  **Feeder** : ingestion des données depuis de fichier au format `.parquet`, et partitionnement par date.

  **Preprocessor** : nettoyage, enrichissement et transformation des données dans Spark via des classes modulaires et fonctions réutilisables, avec un logger intégré pour le suivi.

  **Datamart** : création de tables analytiques dans Hive pour l’exploration et la visualisation.

  **ML** : application de modèles de machine learning sur les données prétraitées, avec sauvegarde des prédictions.

  **API** : exposition de certains résultats ou traitements via une API REST.

  **Visualisation** : génération de graphiques et de recommandations métiers à partir des données traitées.

## Déploiement

  Tous les traitements Spark sont déployés en mode **client** sur YARN, avec configuration des ressources (executors, mémoire, cores) justifiée selon la charge.

  Utilisation de Hive avec Metastore PostgreSQL pour la persistance des metadonnées dans les tables.

## Objectifs métier

   Détecter des tendances de comportement utilisateurs.

   Extraire des KPIs opérationnels (trafic, types d’utilisateurs, anomalies).

   Automatiser des recommandations ou alertes via modèles ML.

**Fonctionnalités principales :**


---

##  Technos utilisés :

Ce projet repose sur les technos suivantes :

* Hadoop 3.2.1
* Spark 3.5.6 + PySpark
* Hive 3.1.3
* PostgreSQL (metastore Hive)
* Docker
* Python 3.10
---

##  Prérequis
* Docker ou la version desktop
* PostgreSQL, avec une Base de donnée (vide!!!) nommée : metastore_hive !!!
* OS Windows (x64/86 et pas ARM), car les images dockers sont sur cette OS!
* Python >3.7, car Pyspark peut mal fonctionner sur YARN si la version de python est <=3.7
---

##  Installation

### 1. Cloner le repo

```bash
git clone https://github.com/ton-utilisateur/projet_bdf3.git
cd projet_bdf3
```

### 2. Lancer les services

```bash
docker compose up -d --build
```

### 3. Initialiser les permissions (si non automatisé)

```bash
docker exec -it namenode bash
hdfs dfs -mkdir -p /tmp/logs
hdfs dfs -chown -R yarn:hadoop /tmp/logs
hdfs dfs -chmod -R 1777 /tmp/logs
```

---

##  Utilisation

### 1. Lancer un job Spark

Placez vos scripts dans `app/`, par exemple.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
print("Hello depuis Spark sur YARN")
spark.stop()
```
car `app/` est monté dans le Volume Docker du container `projet_bdf3` où Spark est installé! C'est dans ce container que vous allez lancer TOUT vos codes Pyspark !

### 2. Rentrer dans le shell du container contenant Spark
```bash
docker exec -it projet_bdf3 bash
```

### 3. Exécuter avec `spark-submit`

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
  --conf spark.hadoop.yarn.resourcemanager.hostname=resourcemanager \
  /app/job_spark1.py
```

### 4. Accéder aux interfaces

* **YARN UI** : [http://localhost:8088](http://localhost:8088)
* **HDFS UI** : [http://localhost:9870](http://localhost:9870)


---
