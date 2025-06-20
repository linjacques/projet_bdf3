import os

class Config:
    # Chemin local pour Temp (données du jour uniquement)
    TEMP_PATH = "/app/bronze_data/temp"

    # Chemin HDFS pour Bronze (données cumulées)
    BRONZE_ROOT = "hdfs://namenode:8020/lakehouse/bronze"

    @staticmethod
    def get_temp_dates():
        """
        Liste les dates disponibles dans TEMP_PATH (local uniquement)
        """
        try:
            return sorted(os.listdir(Config.TEMP_PATH))
        except FileNotFoundError:
            print(f" Dossier temp non trouvé : {Config.TEMP_PATH}")
            return []

    @staticmethod
    def get_parquet_path(base, date_str):
        """
        Construit le chemin parquet pour une date donnée
        """
        return os.path.join(base, date_str, "parquet")
 