from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, to_date, trim, abs, when, lit, sum, year, date_format,
    date_sub, regexp_replace, udf, current_date, collect_list, concat_ws,initcap ) 
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql import DataFrame  # Assurez-vous d'importer DataFrame ici
import os
from datetime import datetime
import logging
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime
import pyspark.sql.functions as func
from pyspark.sql.functions import when, col, lit, to_date, date_add


os.environ['java.library.path'] = "D:/sqljdbc_auth.dll"
jar_path = "C:/Users/DeLL/Desktop/sqljdbc_4.2/enu/jre8/sqljdbc42.jar"
date = datetime.now().strftime("%Y%m%d")

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("BBT") \
    .config("spark.master", "local") \
    .config("spark.driver.extraClassPath", jar_path) \
    .getOrCreate()

# Activer la politique de parsing légacy pour les dates
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Vérification et création des répertoires 
LOG_DIR = "logs"
AUDIT_DIR = "rapports"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

# Configuration des logs
logging.basicConfig(
    filename=f"{LOG_DIR}/transform_report.log",
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
def save_audit_report(filename=f"{AUDIT_DIR}/audit_transform_report.txt"):
    try:
        with open(filename, "w") as file:
            file.writelines(audit_report)
        log_transformation("Rapport d'audit sauvegardé avec succès.", f"Fichier : {filename}")
    except Exception as e:
        log_transformation("Erreur lors de la sauvegarde du rapport d'audit.", str(e), level="error")


# Chargement des données de la couche Bronze
try:
    transactions_df=spark.read.parquet(f"output/bronze/{date}/transactions")
    clients_df=spark.read.parquet(f"output/bronze/{date}/clients")
    magasins_df=spark.read.parquet(f"output/bronze/{date}/magasins")
    produits_df=spark.read.parquet(f"output/bronze/{date}/produits")
    categories_df=spark.read.parquet(f"output/bronze/{date}/categories")
    tax_rates_df = spark.read.parquet("output/bronze/taxes")
    currency_df = spark.read.parquet("output/bronze/exchange")
    
    log_transformation("Chargement des données Bronze réussi")
except Exception as e:
    log_transformation("Erreur lors du chargement des données", details=str(e), level="error")
    raise

transactions_df.show(3)
clients_df.show(3)
produits_df.show(3)
magasins_df.show(3)
categories_df.show(3)

# Fonction pour remplir les valeurs manquantes
def fill_missing_values(df, fill_values):
    for column, value in fill_values.items():
        if column in df.columns:
            df = df.fillna({column: value})
    return df

# Fonction pour gérer les valeurs négatives
def handle_negative_values(df, columns):
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, abs(col(column)))
    return df

# Fonction pour normaliser les données
def normalize_data(df):

    log_transformation("Démarrage de la normalisation des données.", level="info")
    add_to_audit_report("Normalisation", "Démarrage de la normalisation des données.")

    # Valeurs par défaut pour les colonnes
    fill_values = {
        'ShipName': 'Non specifie',
        'ShipCountry': 'Non specifie',
        'Region': 'Non specifie',
        'OrderDate': '2000-01-01',
        'ShipPostalCode': 'Non specifie',
        'ProductName': 'Non specifie',
        'Country': 'Non specifie',
        'CompanyName': 'Non specifie',
        'City': 'Non specifie',
        'PostalCode': 'Non specifie',
        'Fax': 'Non specifie',
        'Phone': 'Non specifie',
        'Freight': 0.0,
        'UnitPrice': 0.0,
        'Discount': 0.0,
        'Quantity': 0,
        'UnitsInStock': 0,
        'ShipAddress': 'Non specifie',
        'CategoryName': 'Non specifie',
        'HomePage': 'Non specifie'
    }
    df = fill_missing_values(df, fill_values)

    # Nettoyage des colonnes de type date
    date_columns = ['OrderDate', 'ShippedDate', 'RequiredDate']
    for col_name in date_columns:
        if col_name in df.columns:
            original_count = df.filter(col(col_name).isNull()).count()
            log_transformation(f"Début de la transformation pour la colonne: {col_name}")

            df = df.withColumn(col_name, F.to_date(F.col(col_name), "MM/dd/yy"))
            df = df.withColumn(col_name, F.date_format(F.col(col_name), "yyyy-MM-dd"))
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"1956", "2022"))
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"1989", "2022"))
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"1996", "2022"))
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"1997", "2023"))
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"1998", "2024"))

            corrected_count = df.filter(col(col_name).isNull()).count()
            log_transformation(
                f"Transformation complétée pour la colonne: {col_name}",
                details=f"{original_count - corrected_count} valeurs corrigées."
            )
            add_to_audit_report("Normalisation", f"Transformation effectuée sur la colonne {col_name}.")

      # Remplir ShippedDate manquante comme 3 jours avant RequiredDate
    if "ShippedDate" in df.columns and "RequiredDate" in df.columns:
        df = df.withColumn(
            'ShippedDate',
            F.when(col('ShippedDate').isNull(), F.date_sub(col('RequiredDate'), 3))
            .otherwise(col('ShippedDate'))
        )
        log_transformation("Remplissage des valeurs manquantes de `ShippedDate` effectué.")

    # Normalisation des types numériques
    numeric_columns = ['UnitPrice', 'Quantity', 'Discount', 'Freight', 'UnitsInStock', 'UnitsOnOrder']
    for col_name in numeric_columns:
        if col_name in df.columns:
            try:
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
                log_transformation(f"Colonne '{col_name}' convertie en type numérique.")
            except Exception as e:
                log_transformation(
                    f"Erreur lors de la conversion de la colonne '{col_name}'",
                    details=str(e),
                    level="error"
                )
    # Nettoyage des colonnes de type String
    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for column in string_columns:
        df = (
            df.withColumn(column, trim(col(column)))  # Supprimer les espaces inutiles au début et à la fin
        )
    # Nettoyage des colonnes de type chaîne
    string_cleaning_rules = {
        "Phone": r"^[0-9.\-\(\) ]+$",
        "CategoryName": r"[^a-zA-Z0-9\s]",
        "Description" :r"[^a-zA-Z0-9\s]",
        "Country": r"[^a-zA-Z0-9\s]"
    }
    for col_name, regex in string_cleaning_rules.items():
        if col_name in df.columns:
            if col_name == "Phone":
                df = df.withColumn(col_name, when(col(col_name).rlike(regex), col(col_name)).otherwise("Non specifie"))
            else:
                df = df.withColumn(col_name, regexp_replace(col(col_name), regex, ""))

    # Mappage de ShipCity à ShipRegion
    if "ShipCity" in df.columns:
        ShipCity_to_ShipRegion = {
            #dic des shipregion
           'Rio de Janeiro': 'RJ',               # État de Rio de Janeiro, Brésil
           'Resende': 'RJ',                      # Aussi dans l'État de Rio de Janeiro
           'San Cristóbal': 'Táchira',           # État de Táchira, Venezuela
           'Albuquerque': 'NM',                  # Nouveau-Mexique, États-Unis
           'Caracas': 'DF',                      # District Fédéral, Venezuela
           'Seattle': 'WA',                      # Washington, États-Unis
           'Lander': 'WY',                       # Wyoming, États-Unis
           'Barquisimeto': 'Lara',               # État de Lara, Venezuela
           'Sao Paulo': 'SP',                    # État de São Paulo, Brésil
           'Cork': 'Co. Cork',                   # Comté de Cork, Irlande
           'Anchorage': 'AK',                    # Alaska, États-Unis
           'Portland': 'OR',                     # Oregon, États-Unis
           'Cowes': 'Isle of Wight',             # Île de Wight, Royaume-Uni
           'Boise': 'ID',                        # Idaho, États-Unis
           'Montréal': 'Québec',                 # Québec, Canada
           'Colchester': 'Essex',                # Comté d'Essex, Royaume-Uni
           'Elgin': 'OR',                        # Oregon, États-Unis
           'Tsawassen': 'BC',                    # Colombie-Britannique, Canada
           'I. de Margarita': 'Nueva Esparta',   # État de Nueva Esparta, Venezuela
           'Campinas': 'SP',                     # État de São Paulo, Brésil
           'Walla Walla': 'WA',                  # Washington, États-Unis
           'Vancouver': 'BC',                    # Colombie-Britannique, Canada
           'Eugene': 'OR',                       # Oregon, États-Unis
           'Kirkland': 'WA',                     # Washington, États-Unis
           'San Francisco': 'CA',                # Californie, États-Unis
           'Butte': 'MT',                        # Montana, États-Unis
           'Reims': 'Grand Est',                 # Région Grand Est, France
           'Münster': 'NW',                      # Rhénanie-du-Nord-Westphalie, Allemagne
           'London': 'LND',                      # Londres, Royaume-Uni
           'Paris': 'IDF',                       # Île-de-France, France
           'Berlin': 'BE',                       # Berlin, Allemagne
           'México D.F.': 'DF',                  # District Fédéral, Mexique
           'Madrid': 'MD',                       # Communauté de Madrid, Espagne
           'Frankfurt': 'HE',                    # Hesse, Allemagne
           'São Paulo': 'SP',                    # État de São Paulo, Brésil
           'Boston': 'MA',                       # Massachusetts, États-Unis
           'Stockholm': 'Stockholm County',      # Comté de Stockholm, Suède
           'Salzburg': 'Salzburg',           # Région de Salzbourg, Autriche
           'Århus': 'Midtjylland',           # Région du Jutland Central, Danemark
           'Cunewalde': 'Saxony',            # Saxe, Allemagne
           'Bern': 'BE',                     # Canton de Berne, Suisse
           'Genève': 'GE',                   # Canton de Genève, Suisse
           'Stavern': 'Vestfold og Telemark',# Norvège
           'Versailles': 'IDF',              # Île-de-France, France
           'Lille': 'Hauts-de-France',       # Hauts-de-France, France
           'Luleå': 'Norrbotten',            # Comté de Norrbotten, Suède
           'Nantes': 'Pays de la Loire',     # Pays de la Loire, France
           'Brandenburg': 'BB',              # Brandebourg, Allemagne
           'Marseille': 'Provence-Alpes-Côte d\'Azur', # PACA, France
           'Oulu': 'Northern Ostrobothnia',  # Ostrobotnie du Nord, Finlande
           'Bergamo': 'Lombardy',            # Lombardie, Italie
           'Graz': 'Styria',                 # Styrie, Autriche
           'Charleroi': 'Wallonia',          # Wallonie, Belgique
           'Bräcke': 'Jämtland',             # Jämtland, Suède
           'Lyon': 'Auvergne-Rhône-Alpes',   # Auvergne-Rhône-Alpes, France
           'Bruxelles': 'Brussels',          # Région de Bruxelles-Capitale, Belgique
           'Barcelona': 'CT',                # Catalogne, Espagne
           'München': 'BY',                  # Bavière, Allemagne
           'Mannheim': 'BW',                 # Bade-Wurtemberg, Allemagne
           'Buenos Aires': 'CABA',           # Buenos Aires, Argentine
           'Aachen': 'NRW',                  # Rhénanie du Nord-Westphalie, Allemagne
           'Strasbourg': 'Grand Est',        # Grand Est, France
           'Stuttgart': 'BW',                # Bade-Wurtemberg, Allemagne
           'Toulouse': 'Occitanie',          # Occitanie, France
           'Helsinki': 'Uusimaa',            # Uusimaa, Finlande
           'Lisboa': 'Lisboa',               # Région de Lisbonne, Portugal
           'Warszawa': 'Mazowieckie',        # Mazovie, Pologne
           'Reggio Emilia': 'Emilia-Romagna',# Émilie-Romagne, Italie
           'Kobenhavn': 'Hovedstaden',       # Capitale, Danemark
           'Torino': 'Piedmont',             # Piémont, Italie
           'Sevilla': 'Andalucía',           # Andalousie, Espagne
           'Köln': 'NRW',                    # Rhénanie du Nord-Westphalie, Allemagne
           'Leipzig': 'Saxony',              # Saxe, Allemagne
           'Frankfurt a.M.': 'HE',           # Hesse, Allemagne  
             
        }
        ShipRegion_update = col("ShipRegion") if "ShipRegion" in df.columns else lit("Non specifie")

        for ShipCity, ShipRegion in ShipCity_to_ShipRegion.items():
            ShipRegion_update = when(col("ShipCity") == ShipCity, ShipRegion).otherwise(ShipRegion_update)

        df = df.withColumn("ShipRegion", ShipRegion_update)
        log_transformation("Mappage de `ShipCity` en `ShipRegion` complété avec succès.")
    else:
        log_transformation("La colonne `ShipCity` n'existe pas dans le DataFrame, le mappage est ignoré.", level="warning")
        add_to_audit_report("Normalisation", "Mappage de `ShipCity` ignoré (colonne manquante).")

    return df

# Supprime les colonnes en doublon
def drop_duplicate_columns(df: DataFrame) -> DataFrame:
    
    #Supprime les colonnes en doublon basées sur leur contenu.Seule la première colonne rencontrée est conservée.
    columns_to_keep = []
    seen_columns = set()

    for col_name in df.columns:
        if col_name in seen_columns:
            continue
        is_duplicate = False
        for kept_col in columns_to_keep:
            try:
                # Vérifie si les contenus sont identiques
                if df.select(col_name).subtract(df.select(kept_col)).count() == 0 and \
                   df.select(kept_col).subtract(df.select(col_name)).count() == 0:
                    is_duplicate = True
                    break
            except Exception as e:
                log_transformation(f"Erreur lors de la comparaison des colonnes {col_name} et {kept_col}", 
                                   details=str(e), level="error")
        
        if not is_duplicate:
            columns_to_keep.append(col_name)
        seen_columns.add(col_name)

    return df.select(*columns_to_keep)

# Transformation complexe : Ajouter les colonnes `code_region`, `statut_client`, `statut_produit`
# Ajouter la colonne `code_region` dans clients
# Dictionnaire de mappage pays -> régions
country_to_region = {
    "EU": ["Germany", "UK", "Sweden", "France", "Spain", "Switzerland", "Austria", "Italy","Ireland", "Belgium", "Portugal", "Norway", "Denmark", "Finland", "Poland","Germani"],
    "AN": ["Mexico", "Canada", "USA"],
    "AS": ["Argentina", "Brazil", "Venezuela"],
    "Asie": ["China", "India", "Japan"],
    "AF": ["Algeria", "Angola", "Kenya", "Morocco", "South Africa"]
}

# Fonction pour ajouter la colonne 'code_region' basée sur le pays
def assign_region_based_on_country(df):
    # Vérifiez que la colonne 'Country' existe
    if "Country" not in df.columns:
        raise ValueError("La colonne 'Country' est manquante dans le DataFrame.")
    
    log_transformation("Démarrage de l'attribution des régions basées sur les pays.")
    add_to_audit_report("Attribution des régions", "Début du traitement.")
    
    # Créer les conditions pour attribuer les régions
    region_conditions = lit("UN")  # Valeur par défaut
    for region, countries in country_to_region.items():
        region_conditions = when(col("Country").isin(countries), lit(region)).otherwise(region_conditions)
    
    # Ajouter la colonne 'Code_region'
    df = df.withColumn("Code_region", region_conditions)
    
    log_transformation("Attribution des régions terminée.")
    add_to_audit_report("Attribution des régions", "Colonne 'Code_region' ajoutée avec succès.")
    return df

# Ajouter la colonne `statut_client` : Segmentation des clients par nb des transac
def statut_client(transactions_df, clients_df):
    # Calculer le nombre de transactions par client
    transaction_count_df = transactions_df.groupBy("CustomerID").count()

    # Joindre les informations sur le statut des clients avec le DataFrame des clients
    df_with_status = clients_df.join(transaction_count_df, on="CustomerID", how="left")
    
    # Ajouter le statut client basé sur le nombre de transactions
    return df_with_status.withColumn(
        "Statut_client",
        when(col("count") >= 50, lit("VIP"))
        .when(col("count").between(15, 50), lit("Régulier"))
        .otherwise(lit("Nouveau"))
    ).drop("count")

# Appliquer la fonction pour ajouter 'statut_client'
try:
    clients_df = statut_client(transactions_df, clients_df)
    log_transformation("Attribution du statut client réussie.")
    add_to_audit_report("Statut client", "Colonne 'Statut_client' ajoutée avec succès.")
except Exception as e:
    log_transformation("Erreur lors de l'attribution du statut client", details=str(e), level="error")
    raise

# 2. Application du statut des produits en fonction des niveaux de stock

def add_statut_produit(transactions_df, produits_df):
    
    try:
        # Calculer la demande totale (quantité totale vendue) par produit
        demande_totale_df = transactions_df.groupBy("ProductID").agg(sum("Quantity").alias("demande_totale"))

        # Joindre la demande totale à la table produits
        produits_df = produits_df.join(demande_totale_df, "ProductID", "left")

        # Ajouter la colonne `Statut_produit` en fonction de la demande totale
        if "demande_totale" in produits_df.columns:
            produits_df = produits_df.withColumn(
                "Statut_produit",
                when(col("demande_totale") >= 500, lit("High Demand"))  # Demande >= 500
                .when(col("demande_totale").between(200, 499), lit("Moderate Demand"))  # Entre 200 et 499
                .when(col("demande_totale").between(10, 199), lit("Low Demand"))  # Entre 10 et 199
                .otherwise(lit("En fin de vie"))  # Demande =< 10 ou null
            ).drop("demande_totale")  # Supprimer la colonne demande_totale après utilisation

            # Log et audit en cas de succès
            log_transformation("Statut des produits mis à jour avec succès.")
            add_to_audit_report("Statut produit", "Colonne 'Statut_produit' ajoutée avec succès.")
        else:
            raise KeyError("La colonne 'demande_totale' est manquante dans produits_df.")
    except KeyError as e:
        log_transformation(f"Erreur : Colonne manquante - {str(e)}", details=str(e), level="error")
        add_to_audit_report("Statut produit", f"Erreur : Colonne manquante - {str(e)}")
        raise
    except Exception as e:
        log_transformation(f"Erreur lors de la mise à jour du statut des produits", details=str(e), level="error")
        add_to_audit_report("Statut produit", f"Erreur : {str(e)}")
        raise
    
    return produits_df

#3. Groupement des produits par catégorie et création d'une liste de produits par catégorie

def group_products_by_category(produits_df, categories_df):

    try:
        # Jointure entre produits et catégories pour enrichir les informations
        produits_enrichis_df = produits_df.join(categories_df, on="CategoryID", how="left")

        # Création de la liste des produits par catégorie
        produits_par_categorie_df = produits_enrichis_df.groupBy("CategoryID", "CategoryName").agg(
            collect_list(concat_ws(":", col("ProductID"), col("ProductName"))).alias("ProduitsParCategorie")
        )

        # Jointure des informations sur les catégories au DataFrame des produits
        produits_df_final = produits_df.join(produits_par_categorie_df, on="CategoryID", how="left")

        # Journalisation et audit
        log_transformation("Regroupement des produits par catégorie réussi.")
        add_to_audit_report(
            "Regroupement produits par catégorie",
            "Colonne 'ProduitsParCategorie' ajoutée avec succès."
        )

        return produits_df_final
    except Exception as e:
        log_transformation(
            "Erreur lors du regroupement des produits par catégorie",
            details=str(e),
            level="error"
        )
        add_to_audit_report(
            "Regroupement produits par catégorie",
            f"Erreur : {str(e)}"
        )
        raise

#Ajout colonne ID_Taxe 
def ajouter_colonne_id_taxe(transactions_df, tax_rates_df):
   
    try:
        # Étape 1 : Préparer les colonnes nécessaires
        transactions_df = transactions_df.withColumn("Annee", year(col("ShippedDate")).cast("int"))
        tax_rates_df = tax_rates_df.withColumn("TaxeRate", col("TaxeRate").cast("float"))
        # Renommer 'Annee' en 'TaxYear' dans tax_rates_df
        tax_rates_df = tax_rates_df.withColumn("TaxeRate", col("TaxeRate").cast("float"))
        tax_rates_df = tax_rates_df.withColumnRenamed("Annee", "Year")
        tax_rates_df = tax_rates_df.withColumn("Year", col("Year").cast("int"))

        # Étape 2 : Joindre les transactions avec les taux de taxe
        transactions_enrichies_df = transactions_df.join(
            tax_rates_df,
            (transactions_df["ShipCountry"] == tax_rates_df["Country"]) &
            (transactions_df["Annee"] == tax_rates_df["Year"]),
            how="left"
        ).select(
            transactions_df["*"],  # Conserver toutes les colonnes de transactions
            tax_rates_df["ID_Taxe"]  # Ajouter uniquement la colonne 'ID_Taxe'
        )

        # Étape 3 : Gérer les valeurs nulles dans la colonne ID_Taxe
        transactions_enrichies_df = transactions_enrichies_df.withColumn(
            "ID_Taxe",
            when(col("ID_Taxe").isNotNull(), col("ID_Taxe")).otherwise(lit("Inconnu"))
        )

        # Étape 4 : Supprimer la colonne temporaire 'Annee'
        transactions_enrichies_df = transactions_enrichies_df.drop("Annee")

        # Log et audit en cas de succès
        log_transformation("Ajout de la colonne 'ID_Taxe' réussi.")
        add_to_audit_report(
            "Ajout de la colonne ID_Taxe",
            "Colonne 'ID_Taxe' ajoutée avec succès en fonction des taux de taxe."
        )

        return transactions_enrichies_df

    except Exception as e:
        log_transformation(
            "Erreur lors de l'ajout de la colonne 'ID_Taxe'",
            details=str(e),
            level="error"
        )
        add_to_audit_report(
            "Ajout de la colonne ID_Taxe",
            f"Erreur : {str(e)}"
        )
        raise

#Ajout colonne ID_Currency 
def ajouter_colonne_id_currency(transactions_df, currency_df):
    
    try:
        # Normalisation des colonnes
        currency_df = currency_df.withColumn("country", trim(col("country")))
        currency_df = currency_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
        transactions_df = transactions_df.withColumn("ShipCountry", trim(col("ShipCountry")))
        # Étape 1 : Joindre les transactions avec le DataFrame currency
        transactions_enrichies_df = transactions_df.join(
            currency_df,
            (transactions_df["ShipCountry"] == currency_df["country"]) &
            (transactions_df["OrderDate"] == currency_df["date"]),
            how="left"
        ).select(
            transactions_df["*"],  # Conserver toutes les colonnes existantes
            currency_df["ID_taux"]  # Ajouter uniquement la colonne 'ID_Currency'
        )

        # Étape 2 : Gérer les valeurs nulles dans 'ID_Currency'
        transactions_enrichies_df = transactions_enrichies_df.withColumn(
            "ID_taux",
            when(col("ID_taux").isNotNull(), col("ID_taux")).otherwise(lit("Inconnu"))
        )

        # Log en cas de succès
        log_transformation("Ajout de la colonne 'ID_Currency' réussi.")
        add_to_audit_report(
            "Ajout de la colonne ID_Currency",
            "Colonne 'ID_Currency' ajoutée avec succès en fonction du pays et de la date."
        )

        return transactions_enrichies_df

    except Exception as e:
        log_transformation(
            "Erreur lors de l'ajout de la colonne 'ID_Currency'",
            details=str(e),
            level="error"
        )
        add_to_audit_report(
            "Ajout de la colonne ID_Currency",
            f"Erreur : {str(e)}"
        )
        raise

#3. Création d'Anomalies et Gestion de la Qualité des Données
# Ajout des anomalies et gestion de la qualité des données
def detect_and_label_anomalies(df):
   
    # Détecter les dates futures
    try:
        df = df.withColumn(
            "anomalie_date_future",
            when(col("OrderDate") > current_date(), lit("Date future")).otherwise(lit(None))
        )
        log_transformation("Anomalies de dates futures détectées avec succès.")
        return df
    except Exception as e:
        log_transformation("Erreur lors de la détection des dates futures", details=str(e), level="error")
        raise

"""
# Ajout d'erreurs intentionnelles
def introduce_intentional_errors(transactions_df, clients_df, produits_df):
    # Produits avec des catégories incohérentes
    if "ProductID" in produits_df.columns and "CategoryID" in produits_df.columns:
        produits_df = produits_df.withColumn(
            "CategoryID",
            when(col("ProductID") % 5 == 0, lit(None)).otherwise(col("CategoryID"))
        )
    else:
        log_and_audit("Colonnes 'ProductID' ou 'CategoryID' manquantes dans produits_df.", level="error")
    
    # Clients avec des adresses manquantes
    if "CustomerID" in clients_df.columns and "Address" in clients_df.columns:
        clients_df = clients_df.withColumn(
            "Address",
            when(col("CustomerID") % 7 == 0, lit(None)).otherwise(col("Address"))
        )
    else:
        log_and_audit("Colonnes 'CustomerID' ou 'Address' manquantes dans clients_df.", level="error")
"""
    # Produits marqués comme actifs sans ventes depuis un an
def update_product_status(transactions_df, produits_df):

    if "ShippedDate" in transactions_df.columns and "ProductID" in transactions_df.columns:
        # Calculer la dernière date de vente pour chaque produit
        ventes_recentes = transactions_df.groupBy("ProductID").agg(
            F.max(to_date("ShippedDate", "yyyy-MM-dd")).alias("DerniereVente")
        )
        # Ajouter cette information au DataFrame des produits
        produits_df = produits_df.join(ventes_recentes, "ProductID", "left")
        produits_df = produits_df.withColumn(
            "statut_ActiviteProduit",
            when(year(current_date()) - year(col("DerniereVente")) > 1, lit("Actif sans ventes"))
            .otherwise("Actif avec ventes")
        ).drop("DerniereVente")  # Supprimer la colonne temporaire après utilisation
    else:
        log_transformation(
            action="Validation des colonnes",
            details="Colonnes nécessaires pour déterminer les ventes manquantes.",
            level="error"
        )
    
    return produits_df

# Appliquer les fonctions sur chaque DataFrame
try:
    negative_columns = ['Quantity', 'UnitPrice', 'Discount', "UnitsInStock", "UnitsOnOrder", "Freight"]

    # Transactions
    log_transformation("Début de la normalisation des données pour le DataFrame transactions.")
    transactions_df = normalize_data(transactions_df)
    log_transformation("Nettoyage des transactions terminée.")
    
    transactions_df = handle_negative_values(transactions_df, negative_columns)
    log_transformation("Traitement des valeurs négatives dans transactions terminé.")

    transactions_df = drop_duplicate_columns(transactions_df)
    log_transformation("Suppression des colonnes dupliquées dans transactions terminée.")

    transactions_df = ajouter_colonne_id_taxe(transactions_df, tax_rates_df)
    log_transformation("Ajout de la colonne ID_Taxe dans transactions terminé.")

    transactions_df = ajouter_colonne_id_currency(transactions_df, currency_df)
    log_transformation("Ajout de la colonne ID_Currency dans transactions terminé.")

    transactions_df = detect_and_label_anomalies(transactions_df)
    log_transformation("Anomalies de dates futures détectées avec succès")

    # Clients
    log_transformation("Début de la normalisation des données pour le DataFrame clients.")
    clients_df = normalize_data(clients_df)
    log_transformation("Nettoyage des clients terminée.")

    clients_df = handle_negative_values(clients_df, negative_columns)
    log_transformation("Traitement des valeurs négatives dans clients terminé.")

    clients_df = assign_region_based_on_country(clients_df)
    log_transformation("Attribution des régions aux clients terminée.")

     # Catégories
    log_transformation("Début de la normalisation des données pour le DataFrame catégories.")
    categories_df = normalize_data(categories_df)
    log_transformation("Nettoyage des catégories terminée.")

    categories_df = handle_negative_values(categories_df, negative_columns)
    log_transformation("Traitement des valeurs négatives dans catégories terminé.")

    # Produits
    log_transformation("Début de la normalisation des données pour le DataFrame produits.")
    produits_df = normalize_data(produits_df)
    log_transformation("Nettoyage des produits terminée.")

    produits_df = handle_negative_values(produits_df, negative_columns)
    log_transformation("Traitement des valeurs négatives dans produits terminé.")

    produits_df = add_statut_produit(transactions_df, produits_df)
    log_transformation("Ajout du statut des produits terminé.")

    produits_df = group_products_by_category(produits_df, categories_df)
    log_transformation("Regroupement des produits par catégorie terminé.")

    produits_df =update_product_status(transactions_df, produits_df)
    log_transformation("le statut des produits en fonction de leur dernière date de vente")

    # Magasins
    log_transformation("Début de la normalisation des données pour le DataFrame magasins.")
    magasins_df = normalize_data(magasins_df)
    log_transformation("Nettoyage des magasins terminée.")

    magasins_df = handle_negative_values(magasins_df, negative_columns)
    log_transformation("Traitement des valeurs négatives dans magasins terminé.")

    log_transformation("Transformation des données exécuté avec succès.")
   
    # Saving the transformed data to Silver layer
    try:
       
        transactions_df.write.mode("overwrite").parquet(f"output/silver/{date}/transactions")
        clients_df.write.mode("overwrite").parquet(f"output/silver/{date}/clients")
        produits_df.write.mode("overwrite").parquet(f"output/silver/{date}/produits")
        magasins_df.write.mode("overwrite").parquet(f"output/silver/{date}/magasins")
        categories_df.write.mode("overwrite").parquet(f"output/silver/{date}/categories")
        tax_rates_df.write.mode("overwrite").parquet(f"output/silver/{date}/Taxes")
        currency_df.write.mode("overwrite").parquet(f"output/silver/{date}/Currency")

        print("Les données transformées ont été sauvegardées dans la couche Silver.")
    except Exception as e:
        log_transformation("Erreur lors de la sauvegarde ",details=str(e), level="error")

except Exception as e:
    log_transformation("Erreur lors de la transformation des données", details=str(e), level="error")
    raise

# Affichage des résultats pour vérification
transactions_df.show(5)
clients_df.show(5)
produits_df.show(5)
magasins_df.show(5)
categories_df.show(5)

# Sauvegarder le rapport d'audit
save_audit_report()

# Arrêter la session Spark
spark.stop()