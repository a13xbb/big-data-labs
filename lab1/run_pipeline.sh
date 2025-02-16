#!/bin/bash
# 1. Поднимаем Kafka
docker-compose up -d

# 2. Ждем, пока Kafka полностью запустится (настройте время ожидания при необходимости)
echo "Waiting for Kafka to start..."
sleep 5
docker exec -it lab1-kafka-1 kafka-topics.sh --create --topic raw_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1
docker exec -it lab1-kafka-1 kafka-topics.sh --create --topic processed_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1
docker exec -it lab1-kafka-1 kafka-topics.sh --create --topic ml_results_data --bootstrap-server localhost:9095 --partitions 3 --replication-factor 1

# 4. Запускаем продюсеров в фоне
echo "Starting Producer 1..."
python3 producers/producer_1.py --data_folder /home/alex/study/big-data-labs/lab1/data &
echo "Starting Producer 2..."
python3 producers/producer_2.py --data_folder /home/alex/study/big-data-labs/lab1/data &
sleep 3

# 3. Запускаем консюмеры в фоне (каждый в отдельном терминальном окне или фоне)
echo "Starting processing consumer..."
python3 consumers/processing_consumer.py --data_folder /home/alex/study/big-data-labs/lab1/data &
sleep 3
echo "Starting ML consumer..."
python3 consumers/ml_consumer.py --model_path /home/alex/study/big-data-labs/lab1/models/catboost_forest_cover_type.model &
sleep 3

echo "Starting visualization consumer..."
streamlit run consumers/visualization.py &

# Ожидаем завершения фоновых процессов (если требуется)
wait