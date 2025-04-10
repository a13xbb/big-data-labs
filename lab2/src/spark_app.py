from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.ml.feature import RobustScaler, VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import DoubleType
from pyspark import StorageLevel
import psutil
import time

def parse_arguments():
    parser = ArgumentParser()

    parser.add_argument('--data-path', '-d')
    parser.add_argument('--optimized', '-o', action='store_true')
    return parser.parse_args()

args = parse_arguments()

if SparkContext._active_spark_context is not None:
    sc = SparkContext.getOrCreate()
    sc.stop()
    
spark = SparkSession.builder \
    .appName("BigDataLab") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

start_time = time.time()
print(f"Initial RAM usage: {psutil.virtual_memory().percent}%")

load_start = time.time()
df = spark.read.csv(args.data_path, header=True, inferSchema=True)
print(f"[Timer] Data load: {time.time() - load_start:.3f} sec")

if args.optimized:
    df = df.repartition(spark.sparkContext.defaultParallelism)

df = df.drop('nameOrig', 'nameDest')

encoding_start = time.time()
categories = df.select("type").distinct().collect()
categories = [row["type"] for row in categories]

for category in categories:
    df = df.withColumn(f"type_{category}", 
                        F.when(F.col("type") == category, 1).otherwise(0))
    
df = df.drop('type')
print(f"[Timer] Encoding: {time.time() - encoding_start:.2f} sec")

if args.optimized:
    scaled_df = df.persist(StorageLevel.MEMORY_AND_DISK)
    df.count()
else:
    scaled_df = df

scaling_start = time.time()
def vector_to_double(v):
    return float(v[0])

vector_to_double_udf = udf(vector_to_double, DoubleType())

columns_norm = ['amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
    
for col_name in columns_norm:
    assembler = VectorAssembler(inputCols=[col_name], outputCol=f"{col_name}_vector")
    scaled_df = assembler.transform(scaled_df)
    
    scaler = RobustScaler(inputCol=f"{col_name}_vector", outputCol=f"{col_name}_scaled")
    scaler_worker = scaler.fit(scaled_df)
    scaled_df = scaler_worker.transform(scaled_df)
    
    scaled_df = scaled_df.withColumn(f"{col_name}_scaled", vector_to_double_udf(scaled_df[f"{col_name}_scaled"]))
    
    scaled_df = scaled_df.drop(f"{col_name}_vector")
    scaled_df = scaled_df.drop(col_name)
print(f"[Timer] Scaling: {time.time() - scaling_start:.2f} sec")


total_time = time.time() - start_time
print(f"[Timer] Total execution time: {total_time:.2f} sec")
print(f"Final RAM usage: {psutil.virtual_memory().percent}%")

spark.stop()