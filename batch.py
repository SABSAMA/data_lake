from pyspark.sql import SparkSession

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Batch Ingestion Pipeline") \
    .getOrCreate()

# Lire les données brutes depuis différentes sources
transactions = spark.read.csv("./raw_data/transactions/", header=True, inferSchema=True)
web_logs = spark.read.text("./raw_data/web_logs/")
social_media = spark.read.json("./raw_data/social_media/")

# Sauvegarder les données dans le Data Lake sans transformations
transactions.write.mode("overwrite").parquet("./raw_data/processed/transactions/")
web_logs.write.mode("overwrite").text("./raw_data/processed/web_logs/")
social_media.write.mode("overwrite").json("./raw_data/processed/social_media/")

print("Batch ingestion terminée!")
spark.stop()
