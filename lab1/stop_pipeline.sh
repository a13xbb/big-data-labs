#!/bin/bash

echo "Stopping all Python processes..."
pkill -f producers/producer_1.py
pkill -f producers/producer_2.py
pkill -f consumers/processing_consumer.py
pkill -f consumers/ml_consumer.py
pkill -f streamlit

echo "Stopping Docker containers..."
docker-compose down

# echo "Cleaning up zombie processes..."
sleep 2

ps -eo ppid,stat | grep -w 'Z' | while read ppid stat; do
    # echo "Killing zombie parent process: $ppid"
    kill -9 "$ppid"
done

echo "All processes stopped."
