import os

class Config:
    TEMP_PATH = "hdfs://namenode:8020/save_hdfs/pre_bronze"
    BRONZE_ROOT = "hdfs://namenode:8020/save_hdfs/union"
    ARGENT_ROOT = "hdfs://namenode:8020/lakehouse/silver/"


    @staticmethod
    def get_temp_dates(spark):
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(Config.TEMP_PATH)

            if not fs.exists(path):
                print(f"Dossier temp HDFS non trouvé : {Config.TEMP_PATH}")
                return []

            statuses = fs.listStatus(path)
            dates = [status.getPath().getName() for status in statuses if status.isDirectory()]
            return sorted(dates)
        except Exception as e:
            print(f"Erreur listage dates dans TEMP_PATH HDFS : {e}")
            return []

    @staticmethod
    def get_parquet_path(base, date_str):
        return os.path.join(base, date_str, "parquet")