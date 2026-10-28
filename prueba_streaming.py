from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, max, min
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, >

spark = SparkSession.builder \
    .appName("ProyectoStreamingSensores") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema que coincide exactamente con generate_sensor_data()
schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

print("--- CONFIGURANDO CONSUMIDOR DE STREAMING DESDE KAFKA ---")
kafka_stream = spark.readStream \
.format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

real_time_stats = parsed_stream.groupBy("sensor_id") \
    .agg(
        avg("temperature").alias("temp_promedio"),
        max("temperature").alias("temp_maxima"),
        avg("humidity").alias("humedad_promedio")
    )

print("--- INICIANDO VISUALIZACIÓN EN TIEMPO REAL ---")
query = real_time_stats.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

