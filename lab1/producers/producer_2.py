import os
import argparse
from kafka_producer import KafkaProducer

def parse_args():
    parser = argparse.ArgumentParser(description="Kafka Producer")
    parser.add_argument(
        '--data_folder', 
        type=str, 
        default='./data',
        help="Path to the data folder"
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    producer = KafkaProducer(df_path=os.path.join(args.data_folder, 'test_2.csv'), producer_id=1, batch_size=32, random_delay=False)
    producer.run()