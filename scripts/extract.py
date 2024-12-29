from pyspark.sql import SparkSession
import os
from datetime import datetime

# Configurer le chemin de la bibliothèque Java
os.environ['java.library.path'] = "D:/sqljdbc_auth.dll"
jar_path = "C:/Users/DeLL/Desktop/sqljdbc_4.2/enu/jre8/sqljdbc42.jar"
date = datetime.now().strftime("%Y%m%d")
# Creer une session Spark
spark = SparkSession.builder \
    .appName("BBT") \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

 # Lecture des fichiers CSV
transactions_df = spark.read.option("header", True).csv("data/raw/transactions.csv")
clients_df = spark.read.option("header", True).csv("data/raw/clients.csv")
magasins_df = spark.read.option("header", True).csv("data/raw/magasins.csv")
produits_df = spark.read.option("header", True).csv("data/raw/produits.csv")
categories_df = spark.read.option("header", True).csv("data/raw/categories.csv")
rates= spark.read.option("header", True).csv("data/raw/taxes.csv")
exchange_df=spark.read.option("header", True).csv("data/raw/exchange.csv")

 # Sauvegarder les fichiers dans la couche Bronze
transactions_df.write.mode("overwrite").parquet(f"output/bronze/{date}/transactions")
clients_df.write.mode("overwrite").parquet(f"output/bronze/{date}/clients")
magasins_df.write.mode("overwrite").parquet(f"output/bronze/{date}/magasins")
produits_df.write.mode("overwrite").parquet(f"output/bronze/{date}/produits")
categories_df.write.mode("overwrite").parquet(f"output/bronze/{date}/categories")
rates.write.mode("overwrite").parquet(f"output/bronze/taxes")
exchange_df.write.mode("overwrite").parquet(f"output/bronze/exchange")
# Stop the Spark session
spark.stop()
