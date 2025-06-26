#  Projet Big Data Framework (BDF3)

##  À propos du projet

Ce projet Big Data a pour objectif de mettre en place une architecture complète de traitement de données dans un contexte proche du réel, depuis l’ingestion jusqu’à l’exploitation des données via des analyses et modèles de machine learning, le tout déployé dans un cluster Hadoop avec Spark sur YARN.

## Architecture générale

Le projet est structuré autour d’une architecture Lambda, avec :

  **Feeder** : ingestion depuis la source de donnée (fichier `.parquet`), partitionnement par date. Et écriture dans HDFS dans la couche bronze

  **Preprocessor** : nettoyage, enrichissement et transformation des données dans Spark via des classes modulaires et fonctions réutilisables, avec un logger intégré pour le suivi. Et écriture dans HDFS dans couche silver

  **Datamart** : création de tables analytiques dans Hive pour l’exploration et la visualisation. Ecriture dans HDFS dans la couche gold

  **ML** : application de modèles de machine learning sur les données prétraitées, avec sauvegarde des prédictions.

  **API** : exposition de certains résultats ou traitements via une API REST.

  **Visualisation** : génération de graphiques et de recommandations métiers à partir des données traitées.

## Objectifs métier

   Détecter des tendances de comportement utilisateurs.

   Extraire des KPIs opérationnels (trafic, types d’utilisateurs, anomalies).

   Automatiser des recommandations ou alertes via modèles ML.

---

##  Technos utilisés :

Ce projet repose sur les technos suivantes :

* Hadoop 3.2.1
* Spark 3.5.6 + PySpark
* Hive 3.1.3
* PostgreSQL + PgAdmin4
* Docker
* Python 3.10
---

##  Prérequis
* Docker ou la version desktop
* PostgreSQL, avec une Base de donnée (vide!!!) nommée : metastore_hive !!!
* OS Windows (x64/86 et pas ARM), car les images dockers sont sur cette OS!
* Python >3.7, car Pyspark peut mal fonctionner sur YARN si la version de python est <=3.7
* téléchargé le dataset suivant `US_Accidents_March23.csv` ici : https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents  et le placer dans `app/source/US_Accidents_March23.csv`
---

##  Installation

### 1. Cloner le repo

```bash
git clone https://github.com/ton-utilisateur/projet_bdf3.git
cd projet_bdf3
```

### 2. Changer les informations de connexion postgres

Dans `app/conf/hive/hive-site.xml` changer les propriétés suivantes pour que Hiveserver puisse se connecter et initialiser le schema plus tard :

```xml
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://host.docker.internal:5432/metastore_hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>votre_user</value>
  </property>
    <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>votre_mdp</value>
  </property>
```

Et dans le `docker-compose.yml` modifier les informations suivantes du container `hive-server`:

```bash
  hive-server:
    ...
    environment:
      - HIVE_METASTORE_DB_TYPE=postgres
      - HIVE_METASTORE_HOST=host.docker.internal:5432
      - HIVE_METASTORE_DB_NAME=votre_bdd
      - HIVE_METASTORE_USER=votre_user
      - HIVE_METASTORE_PASSWORD=votre_mdp
```

le metastore Hive a besoin de se lier à une **Base de donnée relationnelle** pour pouvoir y stocker les métadonnées. Bien vérifier que la Base de données `metastore_hive` existe dans postgres, sinon la connexion va échouer! 

**Attention, Il faut avoir PostgresSQL en client lourd (le logiciel) plutôt qu'une image docker pour ce projet!**

### 2. Construire et Lancer les containers

```bash
docker compose up -d --build
```
**A ce stade, le container `hive-server` ne démarre pas correctement** ou bien il s'éteint au bout de quelques minutes en raison d'une erreur de connexion à postgres ou que le schema Hive n'existe pas. Dans ce cas, il faudra bien lire les étapes suivantes car ils sont là pour debug !

### 3. debug du container `hive-server`
  
  Par défaut, l'image `bde2020/hive:2.3.2-postgresql-metastore` du container vient avec d'ancienne version des jars JDBC de postgres ! On les supprime dans ce projet car ces anciens JDBC semblent ne pas fonctionner avec les version récentes de postgres ! Pour ce projet, nous allons utiliser le JDBC situé dans `app/conf/jars/postgresql-42.7.3.jar` qui est beaucoup plus récent et qui sera automatiquement monté dans le volume du container. 

  **A. Entrer dans le shell du container `hive-server`**
  ```bash
      docker exec -it hive-server bash
  ```

  **B. supprimer les vieilles versions du JDBC postgres**
  ```bash
      rm /opt/hive/lib/postgresql-9.4.1208.jre7.jar   
  ```
  ```bash
      rm /opt/hive/lib/postgresql-jdbc.jar
  ```

  **C. vérifier qu'ils ont été supprimés**
  ```bash
     find /opt/hive/lib -name "postgresql-*.jar"
  ```

  on doit avoir uniquement ces 2 fichiers JAR maintenant : 
  ```bash
  /opt/hive/lib/postgresql-metadata-storage-0.9.2.jar
  /opt/hive/lib/postgresql-42.7.3.jar
  ```

  Une fois les suppressions faites, on va tester la connexion entre hive et postgres. Pour ca on va initialiser le schéma du metastore Hive

  **D. Exécuter le script d'initialisation du schéma Hive metastore**
  ```bash
      schematool -dbType postgres -initSchema
  ```

  **E. Redémarrer le container `hive-server`**
  ```bash
    docker restart hive-server
  ```

Normalement, le schéma et les tables ont étés créées dans la base de données `metastore_hive` de Postgres. Aller dans PgAdmin4 pour voir les tables suivantes :
```
DBS
→ Contient les bases de données Hive

TBLS
→ Contient les tables Hive avec un lien vers DBS

SDS
→ Décrit comment les tables/partitions sont stockées (format de fichier, chemin HDFS, etc.)

COLUMNS_V2
→ Détaille des colonnes des tables (nom, type, position...)

PARTITIONS
→ Informations sur les partitions d’une table (s’il y en a)

PARTITION_KEYS
→ Définit les colonnes de partition d'une table (s’il y en a)

SERDES
→ Sérialiseur/Désérialiseur utilisé (Parquet, ORC...)

TABLE_PARAMS
→ Contient les paramètres personnalisés d'une table (comme les propriétés TBLPROPERTIES).
```
### 4. lancer le serveur Hive 

  **A. Entrer dans le shell du container `hive-server`**
  ```bash
      docker exec -it hive-server bash
  ```

  **A. Lancer le serveur hive**
  ```bash
      hiveserver2
  ```

  et on doit voir ca : 
  ```bash
    SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
    root@ed4ddd3c4cc9:/opt# 
  ```

  le `root@ed4ddd3c4cc9:/opt#` signifie que le serveur Hive fonctionne et qu'on est dans le shell de Hive ! On peut ensuite se connecter à Postgres avec Beeline: 
  ```bash
    beeline -u jdbc:hive2://localhost:10000
  ```

  Et voir le contenu de la Base de donnée ou des tables 
  ```sql
    show databases;
    show tables;
  ```

---

##  Utilisation

### 1. Lancer un job Spark

Placer les scripts dans `app/`, par exemple.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
print("Hello depuis Spark sur YARN")
spark.stop()
```

car `app/` est monté dans le Volume Docker du container `projet_bdf3` où Spark est installé! C'est dans ce container que vous allez lancer TOUT vos codes Pyspark

### 2. Rentrer dans le shell du container `projet_bdf3` (pyspark)

```bash
docker exec -it projet_bdf3 bash
```

### 3. Exécuter `spark-submit` (avec YARN)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
  --conf spark.hadoop.yarn.resourcemanager.hostname=resourcemanager \
  app/job_spark1.py
```

### 4. Résultats 

Vous pourrez voir le résultat de vos jobs spark via les interfaces suivantes :

* **YARN UI** : [http://localhost:8088](http://localhost:8088) -> Pour voir les logs de vos jobs spark

* **HDFS UI** : [http://localhost:9870](http://localhost:9870) -> Pour voir les données stockées dans HDFS

* **pgADMIN4** :  bien surveiller les tables suivantes ->```DBS, TBLS, SDS, COLUMNS_V2, PARTITIONS, PARTITION_KEYS, SERDES, TABLE_PARAMS```

---




