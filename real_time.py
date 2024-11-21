from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("Real-Time Ingestion Pipeline") \
    .getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("transaction_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_method", StringType(), True)
])


kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

filtered_stream = parsed_stream.filter(col("amount") > 0)

query = filtered_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./processed_data/transactions/real_time/") \
    .option("checkpointLocation", "./checkpoints/transactions/") \
    .start()

print("Real-Time ingestion en cours...")
query.awaitTermination()
