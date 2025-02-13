#!/bin/bash
docker-compose up -d

sleep 10

python3 producers/producer_1.py &
python3 producers/producer_2.py &
python3 consumers/processing_consumer.py &
python3 consumers/ml_consumer.py &

wait