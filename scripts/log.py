import os
import logging
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configuration globale
date = datetime.now().strftime("%Y%m%d")
log_file = os.path.join("logs", f"data_logDBrutes_{date}.log")
audit_path = f"rapports/audit_reportDBrutes_{date}.txt"

# Configuration du logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_and_print(message, level="info"):
    if level == "info":
        logging.info(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    print(message)

# Configuration pour les bibliothèques et Spark
os.environ['java.library.path'] = "D:/sqljdbc_auth.dll"
jar_path = "C:/Users/DeLL/Desktop/sqljdbc_4.2/enu/jre8/sqljdbc42.jar"

spark = SparkSession.builder \
    .appName("BBTtest") \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

# Charger les fichiers CSV
file_paths = {
    "transactions": f"data/raw/transactions.csv",
    "clients": f"data/raw/clients.csv",
    "magasins": f"data/raw/magasins.csv",
    "produits": f"data/raw/produits.csv",
    "categories": f"data/raw/categories.csv"
}

dataframes = {}

try:
    for name, path in file_paths.items():
        dataframes[name] = spark.read.option("header", True).csv(path)
    log_and_print("Tous les fichiers CSV ont été chargés avec succès.")
except Exception as e:
    log_and_print(f"Erreur lors du chargement des fichiers CSV : {e}", "error")

# Détection des anomalies
def detect_missing_values(df, name):
    anomalies = []
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts_dict = null_counts.collect()[0].asDict()

    for col, count in null_counts_dict.items():
        if count > 0:
            anomalies.append(f"{count} valeurs nulles dans la colonne '{col}' de {name}")
            log_and_print(f"{count} valeurs nulles détectées dans {name} - colonne '{col}'", "warning")

    return anomalies

def detect_duplicates(df, name):
    anomalies = []
    duplicate_count = df.count() - df.dropDuplicates().count()
    if duplicate_count > 0:
        anomalies.append(f"{duplicate_count} lignes dupliquées détectées dans {name}")
        log_and_print(f"{duplicate_count} lignes dupliquées détectées dans {name}", "warning")
    return anomalies

def detect_invalid_data_types(df, name, column_types):
    anomalies = []
    schema = dict(df.dtypes)
    for col, expected_type in column_types.items():
        if col in df.columns:
            actual_type = schema.get(col)
            if actual_type != expected_type:
                anomalies.append(f"Incohérence de type détectée pour la colonne '{col}' de {name} : attendu {expected_type}, trouvé {actual_type}")
                log_and_print(f"Incohérence de type dans {name} - colonne '{col}': attendu {expected_type}, trouvé {actual_type}", "warning")
    return anomalies

def detect_outliers(df, name, target_column, lower_bound, upper_bound):
    anomalies = []
    if target_column in df.columns:
        outlier_count = df.filter((F.col(target_column) < lower_bound) | (F.col(target_column) > upper_bound)).count()
        if outlier_count > 0:
            anomalies.append(f"{outlier_count} valeurs aberrantes détectées dans la colonne '{target_column}' de {name}")
            log_and_print(f"{outlier_count} valeurs aberrantes détectées dans {name} - colonne '{target_column}'", "warning")
    return anomalies

# Audit des fichiers après ingestion
def report_audit(df, name, column_types, target_column='', lower_bound=None, upper_bound=None):
    audit_report = {}

    # Détection des anomalies de valeurs manquantes
    missing_values = detect_missing_values(df, name)
    audit_report['Valeurs nulles'] = missing_values

    # Détection des doublons
    duplicates = detect_duplicates(df, name)
    audit_report['Doublons'] = duplicates

    # Vérification des incohérences de types
    type_incoherences = detect_invalid_data_types(df, name, column_types)
    audit_report['Incohérences de types'] = type_incoherences

    # Détection des valeurs aberrantes
    if target_column and lower_bound is not None and upper_bound is not None:
        outliers = detect_outliers(df, name, target_column, lower_bound, upper_bound)
        audit_report['Valeurs aberrantes'] = outliers

    return audit_report

# Sauvegarde des fichiers dans la couche Bronze et génération du rapport d'audit
audit_reports = {}

column_types = {
    'ProductID': 'int',  # Exemple : Attendu integer pour 'colonne1'
    'UnitPrice': 'float',  # Exemple : Attendu float pour 'colonne2'
    'Country': 'string'  # Exemple : Attendu string pour 'colonne3'
}

# Spécification de la colonne cible et des bornes pour les valeurs aberrantes
target_column = 'UnitPrice'  # Remplacez par la colonne numérique à analyser
lower_bound = 0  # Limite inférieure
upper_bound = 1000  # Limite supérieure

for name, df in dataframes.items():
    try:
        output_path = f"output/bronze/{name}/{date}/{name}"
        df.write.mode("overwrite").parquet(output_path)
        log_and_print(f"Fichier {name} sauvegardé dans la couche Bronze.")

        # Génération du rapport d'audit
        audit_reports[name] = report_audit(df, name, column_types, target_column, lower_bound, upper_bound)

    except Exception as e:
        log_and_print(f"Erreur lors de l'enregistrement de {name} : {e}", "error")

# Sauvegarde du rapport d'audit
with open(audit_path, 'w') as report_file:
    report_file.write("Rapport d'Audit des Données\n")
    report_file.write("=" * 40 + "\n\n")
    for name, report in audit_reports.items():
        report_file.write(f"Audit Report pour : {name}\n")
        report_file.write("-" * 40 + "\n")
        for key, value in report.items():
            if isinstance(value, list):
                for item in value:
                    report_file.write(f"    - {item}\n")
            else:
                report_file.write(f"{key}: {value}\n")
        report_file.write("\n" + "=" * 40 + "\n\n")

log_and_print(f"Rapport d'audit généré et sauvegardé sous : {audit_path}")
spark.stop()
