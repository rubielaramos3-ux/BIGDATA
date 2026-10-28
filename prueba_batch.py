from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 1. Inicializar sesión de Spark Batch
spark = SparkSession.builder \
    .appName("ProyectoBatchPuestosVotacion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("--- 1. CARGA DEL CONJUNTO DE DATOS PÚBLICO ---")
df = spark.read.option("header", "true").option("inferSchema", "true").csv("PUE>

print("Esquema del CSV original:")
df.printSchema()
df.show(5)

print("--- 2. LIMPIEZA Y TRANSFORMACIÓN ---")
df_clean = df.na.drop()


print("--- 3. ANÁLISIS EXPLORATORIO DE DATOS (EDA) ---")
df_clean.describe().show()

print("--- 4. RESULTADOS BATCH ---")
print(f"Total registros procesados: {df_clean.count()}")

spark.stop()


