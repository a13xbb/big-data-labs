from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce
# from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("BronzeLoader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

bronze_path = "data/bronze/bronze_table"  # путь к delta-таблице или директории, например: "/data/bronze/forest_raw"
df_bronze = spark.read.format("delta").load(bronze_path)

print('RAW TABLE:')
df_bronze.show(5, truncate=False)

#удаляем пропущенные значения
for c in df_bronze.columns:
    df_bronze = df_bronze.withColumn(c, F.when(F.col(c) == " ?", None).otherwise(F.col(c)))
df_bronze = df_bronze.dropna()
print('TABLE AFTER NAN FILTERING:')
df_bronze.show(5, truncate=False)

#удаление дубликатов
df_bronze = df_bronze.dropDuplicates()
print('TABLE AFTER DUPLICATE FILTERING:')
df_bronze.show(5, truncate=False)

#кодирование лейблов
df_bronze = df_bronze.withColumn(
    "label", 
    F.when(df_bronze["label"] == ">50K", 1).otherwise(0)
)
print('TABLE AFTER LABEL ENCODING:')
df_bronze.show(5, truncate=False)

#one-hot кодирование категориальных переменных
categorical_columns = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country']

for col in categorical_columns:
    categories = df_bronze.select(col).distinct().collect()
    categories = [row[col] for row in categories]

    for category in categories:
        df_bronze = df_bronze.withColumn(f"{col}_{category.strip().replace('(', '').replace(')', '')}", 
                            F.when(F.col(col) == category, 1).otherwise(0))
        
    df_bronze = df_bronze.drop(col)
print('TABLE AFTER ONE HOT:')
df_bronze.show(5, truncate=False)

# for col in categorical_columns:
#     unique_values = df_bronze.select(col).distinct().rdd.flatMap(lambda x: x).collect()
#     for value in unique_values:
#         df_bronze = df_bronze.withColumn(f"{col}_{value}", F.when(F.col(col) == value, 1).otherwise(0))
        

# df_bronze.show(5, truncate=False)
# df_bronze.printSchema()

silver_path = "data/silver/silver_table"

#сохранение очищенных данных в silver-слой
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .save(silver_path)        
    
df_silver = spark.read.format("delta").load(silver_path)
df_silver.show(5, truncate=False)
df_silver.printSchema()




