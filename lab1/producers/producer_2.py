import os
from kafka_producer import KafkaProducer

data_folder = '/home/alex/study/big-data-labs/lab1/data'

producer = KafkaProducer(df_path=os.path.join(data_folder, 'test_2.csv'), producer_id=1, batch_size=32, random_delay=False)

if __name__ == '__main__':
    producer.run()