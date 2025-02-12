import os
from kafka_producer import KafkaProducer

data_folder = '/home/alex/Study/big-data-labs/lab1/data'

producer = KafkaProducer(df_path=os.path.join(data_folder, 'test_1.csv'), producer_id=0, batch_size=64, random_delay=True)

if __name__ == '__main__':
    producer.run()

