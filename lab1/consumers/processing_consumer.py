from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import numpy as np
import pandas as pd
import argparse
from sklearn.preprocessing import StandardScaler
import os

def parse_args():
    parser = argparse.ArgumentParser(description="Processing Consumer")
    parser.add_argument(
        '--data_folder', 
        type=str, 
        default='./data',
        help="Path to the data folder"
    )
    return parser.parse_args()

conf_consumer = {
    'bootstrap.servers': 'localhost:9095',
    'group.id': 'data-processing-group',
    'auto.offset.reset': 'earliest'
}

conf_producer = {
    'bootstrap.servers': 'localhost:9095',
    'client.id': 'processed_data_producer'
}

args = parse_args()

consumer = Consumer(conf_consumer)

consumer.subscribe(['raw_data'])

scaler = StandardScaler()
train_df = pd.read_csv(os.path.join(args.data_folder, 'train.csv'))
if 'Unnamed: 0' in train_df.columns:
    train_df = train_df.drop(columns=['Unnamed: 0'])
scaler.fit(train_df.drop(columns=['Cover_Type']))

producer = Producer(conf_producer)

processed_topic = 'processed_data'

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        
        data = json.loads(msg.value().decode('utf-8'))
        df = pd.DataFrame(data)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
    
        X = df.drop(columns=['Cover_Type'])
        y = df['Cover_Type'].to_numpy()
        
        X_norm = scaler.transform(X)

        processed_data = {
            'normalized_features': X_norm.tolist(),
            'y_true': y.tolist()
        }

        producer.produce(processed_topic, key=msg.key(), value=json.dumps(processed_data))
        producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close() 

