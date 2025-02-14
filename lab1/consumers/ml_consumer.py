from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import json
import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

model_path = '/home/alex/study/big-data-labs/lab1/models/catboost_forest_cover_type.model'

conf_consumer = {
    'bootstrap.servers': 'localhost:9095',  # Адрес первого брокера
    'group.id': 'ml-group',
    'auto.offset.reset': 'earliest'  # Начнем с самого начала
}

conf_producer = {
    'bootstrap.servers': 'localhost:9095',  # Адрес нового брокера
    'client.id': 'ml_results_producer'
}

model = CatBoostClassifier()
model.load_model(model_path)

consumer = Consumer(conf_consumer)

consumer.subscribe(['processed_data'])

producer = Producer(conf_producer)

ml_results_topic = 'ml_results'

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
        
        X = np.array(data['normalized_features'])
  
        y_true = np.array(data['y_true'])
        
        y_pred = np.array(model.predict(X)).flatten()

        # from sklearn.metrics import accuracy_score
        # print(y_true, y_pred, accuracy_score(y_true, y_pred), sep='\n')
        # exit(0)
        
        processed_data = {
            'y_pred': y_pred.tolist(),
            'y_true': y_true.tolist()
        }

        # Отправляем обработанные данные на второй брокер в топик processed_data
        producer.produce(ml_results_topic, key=msg.key(), value=json.dumps(processed_data))
        producer.flush()  # Убедимся, что все сообщения отправлены

        # print(f"Processed and sent data to topic {ml_results_topic}")

        
except KeyboardInterrupt:
    pass
finally:
    consumer.close() 