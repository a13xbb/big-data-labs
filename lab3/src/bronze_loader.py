from pyspark.sql import SparkSession
import logging
# from delta import configure_spark_with_delta_pip

# Создание SparkSession с поддержкой Delta Lake
logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)

builder = SparkSession.builder \
    .appName("BronzeLoader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

csv_path = "./data/adult.csv"
delta_bronze_path = "./data/bronze/bronze_table"

df_raw = spark.read.csv(csv_path, header=True, inferSchema=True)

df_raw.write.format("delta").mode("overwrite").save(delta_bronze_path)

print("✅ Данные успешно сохранены в Delta формате (bronze слой).")

spark.stop()