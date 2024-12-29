import logging
import os
import datetime
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat_ws, lit, current_date, when
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DateType
from log import log_and_print 
from pyspark.sql.functions import monotonically_increasing_id 

# Configuration pour les bibliothèques et Spark
os.environ['java.library.path'] = "D:/sqljdbc_auth.dll"
jar_path = "C:/Users/DeLL/Desktop/sqljdbc_4.2/enu/jre8/sqljdbc42.jar"
date = datetime.now().strftime("%Y%m%d")

# Créer une session Spark
def create_spark_session():
    return SparkSession.builder \
    .appName("BBT") \
    .config("spark.master", "local") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


# Vérification et création des répertoires 
LOG_DIR = "logs"
AUDIT_DIR = "rapports"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

# Configuration des logs
logging.basicConfig(
    filename=f"{LOG_DIR}/load_report.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"  # Ecrase les logs à chaque exécution
)

# Liste pour le rapport d’audit
audit_report = []

# Fonction pour enregistrer les transformations
def log_transformation(action, details=None, level="info", verbose=True):
    message = f"{datetime.now()} - {action}"
    if details:
        message += f" - Détails : {details}"
    
    # Enregistrement dans les logs
    if level == "info":
        logging.info(message)
    elif level == "warning":
        logging.warning(message)
    elif level == "error":
        logging.error(message)
    
    # Optionnel : affichage dans la console
    if verbose:
        print(f"[LOG] {message}")
        

# Fonction pour ajouter des détails au rapport d’audit
def add_to_audit_report(section, details):
    entry = f"{datetime.now()} - {section}: {details}\n"
    audit_report.append(entry)
    log_transformation(f"Rapport d'audit mis à jour pour la section {section}", details)

# Fonction pour sauvegarder le rapport d’audit
def save_audit_report(filename=f"{AUDIT_DIR}/audit_load_report.txt"):
    try:
        with open(filename, "w") as file:
            file.writelines(audit_report)
        log_transformation("Rapport d'audit sauvegardé avec succès.", f"Fichier : {filename}")
    except Exception as e:
        log_transformation("Erreur lors de la sauvegarde du rapport d'audit.", str(e), level="error")



# Fonction pour sauvegarder un DataFrame dans un format spécifique
def save_dataframe(df, path, format="parquet", mode="overwrite"):
    try:
        log_transformation(f"Enregistrement des données au chemin : {path}, format : {format}")
        df.write.mode(mode).format(format).save(path)
        log_transformation(f"Les données ont été enregistrées avec succès dans : {path}")
    except Exception as e:
        log_transformation(f"Erreur lors de l'enregistrement dans {path} : {str(e)}", level="error")
        raise


# Fonction pour sauvegarder les données dans une base de données relationnelle
def save_to_database(df, table_name, jdbc_url, mode="overwrite"):
    try:
        # Convertir les colonnes de type Array en chaînes de caractères
        for column in df.columns:
            if isinstance(df.schema[column].dataType, ArrayType):
                df = df.withColumn(column, concat_ws(",", col(column)))

        log_transformation(f"Enregistrement des données dans la table : {table_name}")
        df.write.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        log_transformation(f"Données enregistrées dans la table : {table_name}")
    except Exception as e:
        log_transformation(f"Erreur lors de l'enregistrement dans la table {table_name} : {str(e)}", level="error")
        raise


# Fonction pour créer la dimension clients avec SCD Type 2
def create_dim_client(clients_df):
    try: 
        # Ajouter les colonnes `create_date`, `update_date`, et `end_date`
        if "update_date" not in clients_df.columns:
            clients_df = clients_df.withColumn("update_date", current_date())
            log_transformation("Colonne 'update_date' ajoutée avec la valeur par défaut (current_date).")

        if "create_date" not in clients_df.columns:
            clients_df = clients_df.withColumn("create_date", current_date())
        
        if "end_date" not in clients_df.columns:
            clients_df = clients_df.withColumn("end_date", lit(None).cast("date"))

        # Ajouter les colonnes pour SCD Type 2
        dim_client = clients_df \
            .withColumn("is_current", when(col("end_date").isNull(), lit(1)).otherwise(lit(0))) \
            .withColumn("start_date", col("create_date")) \
            .withColumn("end_date", when(col("is_current") == 0, col("update_date")).otherwise(col("end_date")))
        log_transformation("Dimension SCD2 créée avec succès.")
        return dim_client
    except Exception as e:
        log_transformation("Erreur lors de la création de la dimension SCD2", details=str(e), level="error")
        raise
    

# Fonction pour créer la dimension produits avec SCD Type 2
def create_dim_produits(produits_df):
    try:
        # Ajouter les colonnes `create_date`, `update_date`, et `end_date`
        if "update_date" not in produits_df.columns:
            produits_df = produits_df.withColumn("update_date", current_date())
            log_transformation("Colonne 'update_date' ajoutée avec la valeur par défaut (current_date).")

        if "create_date" not in produits_df.columns:
            produits_df = produits_df.withColumn("create_date", current_date())
        
        if "end_date" not in produits_df.columns:
            produits_df = produits_df.withColumn("end_date", lit(None).cast("date"))
        
        # Ajouter les colonnes pour SCD Type 2
        dim_produit = produits_df \
            .withColumn("is_current", when(col("end_date").isNull(), lit(1)).otherwise(lit(0))) \
            .withColumn("start_date", col("create_date")) \
            .withColumn("end_date", when(col("is_current") == 0, col("update_date")).otherwise(col("end_date")))
        log_transformation("Dimension SCD2 créée avec succès.")
        return dim_produit
    except Exception as e:
        log_transformation("Erreur lors de la création de la dimension SCD2", details=str(e), level="error")
        raise

# Fonction pour enrichir les transactions avec les ID manquants
def ajout_IDMagasin(fact_transaction, produits_df):
    try:
        fact_transaction = fact_transaction.join(
            produits_df.select("ProductID", "SupplierID", "CategoryID"),
            fact_transaction["ProductID"] == produits_df["ProductID"],
            "left"
        ).drop(produits_df["ProductID"])

        fact_transaction = fact_transaction.withColumn(
            "SupplierID",
            when(col("SupplierID").isNotNull(), col("SupplierID")).otherwise(lit(1))
        )

        fact_transaction = fact_transaction.withColumn(
            "CategoryID",
            when(col("CategoryID").isNotNull(), col("CategoryID")).otherwise(lit(1))
        )

        log_transformation("Transactions enrichies avec les IDs manquants.")
        return fact_transaction
    except Exception as e:
        log_transformation("Erreur lors de l'enrichissement des transactions", details=str(e), level="error")
        raise



# Main
if __name__ == "__main__":
    # Initialiser la session Spark
    spark = create_spark_session()

    # Configuration JDBC pour SQL Server
    jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=BBT projet;integratedSecurity=true"
    jdbc_properties = {
    "user": "DESKTOP-F60H8N1/DeLL",
    "password": "",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
     }

    # Charger les données
    fact_transaction = spark.read.parquet(f"output/silver/{date}/transactions")
    clients_df = spark.read.parquet(f"output/silver/{date}/clients")
    produits_df = spark.read.parquet(f"output/silver/{date}/produits")
    dim_magasin = spark.read.parquet(f"output/silver/{date}/magasins")
    dim_categorie = spark.read.parquet(f"output/silver/{date}/categories")
    dim_currency = spark.read.parquet(f"output/silver/{date}/Currency")
    dim_taxe = spark.read.parquet(f"output/silver/{date}/Taxes")

    add_to_audit_report("Chargement des données", "Toutes les tables silver ont été chargées avec succès.")

    # Transformer les données
    dim_client = create_dim_client(clients_df)
    dim_produit = create_dim_produits(produits_df)
    fact_transaction = ajout_IDMagasin(fact_transaction, produits_df)
    # Ajouter un identifiant unique (id_transaction) en utilisant monotonically_increasing_id
    fact_transaction = fact_transaction.withColumn(
    "id_transaction", monotonically_increasing_id()+ 1
    )
    
    dim_currency=(dim_currency.filter(col("ID_taux").isNotNull()).
    select(
        col("ID_taux").cast("float"),
        col("date"),
        col("ratetoeuro"),
        col("Country"),
        col("currency")
    )
    )
    
   
    
    
    add_to_audit_report("Transformation des données", "Dimensions créées avec succès.")

    # Sauvegarder les données dans le système de fichiers
    save_dataframe(fact_transaction, f"output/gold/{date}/transactions", format="parquet")
    save_dataframe(dim_client, f"output/gold/{date}/clients", format="parquet")
    save_dataframe(dim_produit, f"output/gold/{date}/produits", format="parquet")
    save_dataframe(dim_magasin, f"output/gold/{date}/magasins", format="parquet")
    save_dataframe(dim_categorie, f"output/gold/{date}/categories", format="parquet")
    save_dataframe(dim_taxe, f"output/gold/{date}/taxes", format="parquet")
    save_dataframe(dim_currency, f"output/gold/{date}/Currency", format="parquet")

    add_to_audit_report("Enregistrement", "Données sauvegardées avec succès dans les fichiers gold.")

   # Sauvegarder les données dans la base de données
    save_to_database(dim_categorie,"Dim_Categorie",jdbc_url)
    save_to_database(dim_client,"Dim_Client",jdbc_url)
    save_to_database(dim_produit,"Dim_Produit",jdbc_url)
    save_to_database(dim_taxe,"Dim_Taxe",jdbc_url)
    save_to_database(dim_magasin,"Dim_Magasin",jdbc_url)
    save_to_database(dim_currency, "Dim_Currency", jdbc_url)
    save_to_database(fact_transaction,"Fact_Transaction",jdbc_url)
  

    add_to_audit_report("Base de données", "Données sauvegardées avec succès dans la base de données.")

    log_transformation("Chargement terminé avec succès.")

    # Sauvegarder le rapport d'audit
    save_audit_report()

    spark.stop()