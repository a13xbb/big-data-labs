docker compose down

sudo rm -rf data/bronze
sudo rm -rf data/silver
sudo rm -rf data/gold

sudo mkdir data/bronze
sudo mkdir data/silver
sudo mkdir data/gold

docker compose up -d --build

mlflow ui --port 5001

docker exec -t spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties" \
 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
 --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  bronze_loader.py

 docker exec -t spark spark-submit --packages io.delta:delta-spark_2.12:3.2.0 \
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties" \
 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
 --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  etl_pipeline.py