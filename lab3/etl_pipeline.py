from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from functools import reduce
from delta.tables import DeltaTable
import time

import xgboost as xgb
import mlflow
import mlflow.xgboost
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, log_loss
import matplotlib.pyplot as plt

start_time = time.time()

builder = SparkSession.builder \
    .appName("Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

bronze_path = "/app/data/bronze/bronze_table"  # путь к delta-таблице или директории, например: "/data/bronze/forest_raw"

delta_table = DeltaTable.forPath(spark, bronze_path)
spark.sql(f"""
    OPTIMIZE delta.`{bronze_path}`
    ZORDER BY (label)
""")
delta_table.optimize().executeCompaction()

df_bronze = spark.read.format("delta").load(bronze_path)

df_bronze = df_bronze.repartition(spark.sparkContext.defaultParallelism)

# print('RAW TABLE:')
# df_bronze.show(5, truncate=False)

#удаляем пропущенные значения
for c in df_bronze.columns:
    df_bronze = df_bronze.withColumn(c, F.when(F.col(c) == " ?", None).otherwise(F.col(c)))
df_bronze = df_bronze.dropna()
df_bronze = df_bronze.persist(StorageLevel.MEMORY_AND_DISK)
# print('TABLE AFTER NAN FILTERING:')
# df_bronze.show(5, truncate=False)

#удаление дубликатов
df_bronze = df_bronze.dropDuplicates()
# print('TABLE AFTER DUPLICATE FILTERING:')
# df_bronze.show(5, truncate=False)

#кодирование лейблов
df_bronze = df_bronze.withColumn(
    "label", 
    F.when(df_bronze["label"].isin([" >50K", " >50K."]), 1).otherwise(0)
)
# print('TABLE AFTER LABEL ENCODING:')
# df_bronze.show(5, truncate=False)

#one-hot кодирование категориальных переменных
categorical_columns = ['workclass', 'education', 'marital-status', 'occupation', 'relationship', 'race', 'sex', 'native-country']

for col in categorical_columns:
    categories = df_bronze.select(col).distinct().collect()
    categories = [row[col] for row in categories]

    for category in categories:
        df_bronze = df_bronze.withColumn(f"{col}_{category.strip().replace('(', '').replace(')', '')}", 
                            F.when(F.col(col) == category, 1).otherwise(0))
        
    df_bronze = df_bronze.drop(col)
# print('TABLE AFTER ONE HOT:')
# df_bronze.show(5, truncate=False)

silver_path = "data/silver/silver_table"

#сохранение очищенных данных в silver-слой
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .save(silver_path)        
    
df_bronze.unpersist()

print(f'Data processing execution time: {time.time() - start_time}')


#Обучение модели на данных из silver слоя
df_silver = spark.read.format("delta").load(silver_path)
df_silver.show(5, truncate=False)
df_silver.printSchema()

# Разделим на признаки (все колонки, кроме 'label')
feature_columns = [col for col in df_silver.columns if col != 'label']

# Преобразуем признаки в вектор
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

# Создаём DataFrame с признаками и целевой переменной
df_processed = assembler.transform(df_silver)
df_processed = df_processed.select("features", "label")



mlflow.set_tracking_uri("file:/app/mlruns")

train_data = df_processed.select("features", "label").toPandas()

# train_data.to_csv('train.csv')
# exit(0)
X = train_data["features"].tolist()
y = train_data["label"].tolist()

# Разделяем данные на train и validation
X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.3, random_state=42)

# Параметры для XGBoost
params = {
    "objective": "binary:logistic",
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "base_score": 0.5,
    "eval_metric": ["logloss", "error", 'aucpr']
}

# Логирование в MLflow
mlflow.set_tracking_uri("file:/app/mlruns")
# mlflow.set_tracking_uri("http://localhost:5000")

with mlflow.start_run():
    # Обучаем модель
    model = xgb.XGBClassifier(**params)

    # Обучение с логированием на каждой эпохе
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_valid, y_valid)],
        verbose=True  # чтобы выводилось каждое дерево
    )
    
    mlflow.log_params(params)

    # Логируем финальные метрики на трейне и валидации
    y_train_pred = model.predict(X_train)
    y_valid_pred = model.predict(X_valid)

    train_acc = accuracy_score(y_train, y_train_pred)
    valid_acc = accuracy_score(y_valid, y_valid_pred)
    train_f1 = f1_score(y_train, y_train_pred)
    valid_f1 = f1_score(y_valid, y_valid_pred)
    
    y_train_prob = model.predict_proba(X_train)[:, 1]
    y_valid_prob = model.predict_proba(X_valid)[:, 1]

    train_loss = log_loss(y_train, y_train_prob)
    valid_loss = log_loss(y_valid, y_valid_prob)

    mlflow.log_metrics({
        "train_accuracy": train_acc,
        "valid_accuracy": valid_acc,
        "train_f1": train_f1,
        "valid_f1": valid_f1,
        "train_loss": train_loss,
        "valid_loss": valid_loss
    })

    print("Metrics logged to MLflow!")

    # Сохраняем эволюцию ошибок на каждой итерации
    evals_result = model.evals_result()
    
    for metric_name in evals_result['validation_0'].keys():
        plt.figure(figsize=(10,6))
        plt.plot(evals_result['validation_0'][metric_name], label=f"Train {metric_name}")
        plt.plot(evals_result['validation_1'][metric_name], label=f"Valid {metric_name}")
        plt.title(f"{metric_name} over iterations")
        plt.xlabel("Iteration")
        plt.ylabel(metric_name)
        plt.legend()
        plt.grid()

        # Сохраним график во временный файл
        plot_path = f"{metric_name}_plot.png"
        plt.savefig(plot_path)
        plt.close()

        # Логируем график в MLflow
        # mlflow.log_artifact(plot_path)


