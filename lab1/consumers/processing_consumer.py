from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

conf_consumer = {
    'bootstrap.servers': 'localhost:9095',  # Адрес первого брокера
    'group.id': 'data-processing-group',
    'auto.offset.reset': 'earliest'  # Начнем с самого начала
}

conf_producer = {
    'bootstrap.servers': 'localhost:9095',  # Адрес нового брокера
    'client.id': 'processed_data_producer'
}

consumer = Consumer(conf_consumer)

consumer.subscribe(['raw_data'])

scaler = StandardScaler()
train_df = pd.read_csv('/home/alex/study/big-data-labs/lab1/data/train.csv')
if 'Unnamed: 0' in train_df.columns:
    train_df = train_df.drop(columns=['Unnamed: 0'])
scaler.fit(train_df.drop(columns=['Cover_Type']))

producer = Producer(conf_producer)

processed_topic = 'processed_data'

try:
    while True:
        msg = consumer.poll(1.0)  # Ожидаем сообщение 1 секунду

        if msg is None:
            continue  # Нет новых сообщений, продолжаем
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        # Получаем ключ и тело сообщения
        # print(f"Received message from producer: {msg.key().decode('utf-8')}")
        data = json.loads(msg.value().decode('utf-8'))  # Десериализуем данные
        
        df = pd.DataFrame(data)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
    
        X = df.drop(columns=['Cover_Type'])
        y = df['Cover_Type'].to_numpy()
        
        X_norm = scaler.transform(X)


        # Создаем новое сообщение с обработанными данными
        processed_data = {
            'normalized_features': X_norm.tolist(),
            'y_true': y.tolist() # Преобразуем обратно в список
        }
        
        # print(X_norm.shape)

        # Отправляем обработанные данные на второй брокер в топик processed_data
        producer.produce(processed_topic, key=msg.key(), value=json.dumps(processed_data))
        producer.flush()  # Убедимся, что все сообщения отправлены

        # print(f"Processed and sent data to topic {processed_topic}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close() 

