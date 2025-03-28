import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.ml.feature import RobustScaler, VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import DoubleType
import psutil
import time

findspark.init("/usr/local/spark")

if SparkContext._active_spark_context is not None:
    sc = SparkContext.getOrCreate()
    sc.stop()
    
spark = SparkSession.builder \
    .appName("BigDataLab") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.ui.showConsoleProgress", "true") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

start_time = time.time()
print(f"Initial RAM usage: {psutil.virtual_memory().percent}%")

df = spark.read.csv("hdfs://localhost:9000/user/hadoop/fraud.csv", header=True, inferSchema=True)

df = df.drop('nameOrig', 'nameDest')

categories = df.select("type").distinct().collect()
categories = [row["type"] for row in categories]

for category in categories:
    df = df.withColumn(f"type_{category}", 
                        F.when(F.col("type") == category, 1).otherwise(0))
    
df = df.drop('type')

def vector_to_double(v):
    return float(v[0])

vector_to_double_udf = udf(vector_to_double, DoubleType())

columns_norm = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']

scaled_df = df
for col_name in columns_norm:
    assembler = VectorAssembler(inputCols=[col_name], outputCol=f"{col_name}_vector")
    scaled_df = assembler.transform(scaled_df)
    
    scaler = RobustScaler(inputCol=f"{col_name}_vector", outputCol=f"{col_name}_scaled")
    scaler_worker = scaler.fit(scaled_df)
    scaled_df = scaler_worker.transform(scaled_df)
    
    scaled_df = scaled_df.withColumn(f"{col_name}_scaled", vector_to_double_udf(scaled_df[f"{col_name}_scaled"]))
    
    scaled_df = scaled_df.drop(f"{col_name}_vector")
    scaled_df = scaled_df.drop(col_name)
    
print(f"Execution time: {time.time() - start_time:.2f} sec")
print(f"Final RAM usage: {psutil.virtual_memory().percent}%")

spark.stop()