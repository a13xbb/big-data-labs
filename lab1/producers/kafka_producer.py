import json
import time
import pandas as pd
import random
from confluent_kafka import Producer

TOPIC = "raw_data"

class KafkaProducer:
    def __init__(self, df_path, producer_id, batch_size=32, random_delay=False):
        self.df = pd.read_csv(df_path)
        self.conf = {
            'bootstrap.servers': 'localhost:9095',  # Адрес первого брокера
            'client.id': f'producer_{producer_id}'
        }
        # print(self.df.columns)
        self.producer = Producer(self.conf)
        self.producer_id = producer_id
        self.random_delay = random_delay 
        self.batch_size = batch_size
        
    def handle_error(self, err, msg):
        if err:
            print(f"Producer {self.producer_id}: Error while sending - {err}")
        else:
            print(f"Producer {self.producer_id}: Sent - {msg.value().decode('utf-8')}")
            
    def run(self):
        for i in range(0, len(self.df), self.batch_size):
            batch = self.df.iloc[i:min(i + self.batch_size, len(self.df))]  # Берем батч данных
            messages = batch.to_dict(orient="records")  # Преобразуем в список JSON-объектов
            self.producer.produce(TOPIC, key=f'producer_{self.producer_id}', value=json.dumps(messages))
            self.producer.flush() 
            
            delay = random.uniform(0.05, 0.5) if self.random_delay else 0.15
            time.sleep(delay)
            
        print((f"Producer {self.producer_id} finished sending data"))