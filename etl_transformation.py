from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("ETL Transformation Pipeline") \
    .getOrCreate()

# Lire les données brutes depuis le Data Lake après ingestion en batch
transactions = spark.read.parquet("./raw_data/processed/transactions/")
web_logs = spark.read.text("./raw_data/processed/web_logs/")
social_media = spark.read.json("./raw_data/processed/social_media/")

# Transformation des données

# 1. Transactions - Filtrer les transactions avec un montant positif
transactions_transformed = transactions.filter(transactions["amount"] > 0)

# 2. Web Logs - Extraire les informations et renommer la colonne
web_logs_transformed = web_logs.withColumnRenamed("value", "log_entry") \
    .withColumn("log_date", to_date(col("log_entry").substr(1, 10), "yyyy-MM-dd"))

# 3. Social Media - Filtrer les entrées sans email ou contenu
social_media_transformed = social_media.filter(social_media["email"].isNotNull()) \
    .filter(social_media["content"].isNotNull())

# Sauvegarder les données transformées
transactions_transformed.write.mode("overwrite").parquet("./processed_data/transactions/")
web_logs_transformed.write.mode("overwrite").text("./processed_data/web_logs/")
social_media_transformed.write.mode("overwrite").json("./processed_data/social_media/")

print("Transformation et nettoyage terminés!")
spark.stop()
