docker compose down

sudo rm -rf data/bronze/bronze_table
sudo rm -rf data/silver/silver_table
sudo rm -rf data/gold/gold_table

docker compose up -d --build

# docker exec -t spark pip install numpy
# docker exec -t spark pip install xgboost
# docker exec -t spark pip install mlflow

mlflow ui

docker exec -t spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties" \
 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
 --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  bronze_loader.py

 docker exec -t spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties" \
 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
 --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  etl_pipeline.py