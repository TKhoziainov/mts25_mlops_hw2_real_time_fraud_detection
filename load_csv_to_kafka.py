#!/usr/bin/env python3
import pandas as pd
from kafka import KafkaProducer
import json
import argparse
import uuid
import os
import sys

def load_csv_to_kafka(csv_path, kafka_brokers, topic, batch_size=100):
    """
    Загружает данные из CSV файла в топик Kafka
    
    Args:
        csv_path: путь к CSV файлу
        kafka_brokers: адреса Kafka брокеров (например, "localhost:9095" или "kafka:9092")
        topic: название топика Kafka
        batch_size: размер батча для отправки (по умолчанию 100)
    """
    try:
        print(f"Загрузка CSV файла: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Загружено строк: {len(df)}")
        
        print(f"Подключение к Kafka: {kafka_brokers}")
        brokers_list = kafka_brokers.split(',') if isinstance(kafka_brokers, str) else kafka_brokers
        producer = KafkaProducer(
            bootstrap_servers=brokers_list,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT",
            api_version=(0, 10, 1),
            request_timeout_ms=30000,
            retries=3
        )
        
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        print(f"Отправка данных в топик '{topic}'...")
        sent_count = 0
        
        for idx, row in df.iterrows():
            message = {
                "transaction_id": row['transaction_id'],
                "data": row.drop('transaction_id').to_dict()
            }
            
            producer.send(topic, value=message)
            sent_count += 1
            
            if sent_count % batch_size == 0:
                print(f"Отправлено {sent_count}/{len(df)} сообщений...")
                producer.flush()
        
        producer.flush()
        print(f"✓ Успешно отправлено {sent_count} сообщений в топик '{topic}'")
        return True
        
    except FileNotFoundError:
        print(f"Ошибка: файл '{csv_path}' не найден", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Ошибка при загрузке данных: {str(e)}", file=sys.stderr)
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Загрузка данных из CSV файла в топик Kafka'
    )
    parser.add_argument(
        'csv_path',
        type=str,
        help='Путь к CSV файлу'
    )
    parser.add_argument(
        '--kafka-brokers',
        type=str,
        default=os.getenv('KAFKA_BROKERS', 'localhost:9095'),
        help='Адреса Kafka брокеров (по умолчанию: localhost:9095 или из переменной окружения KAFKA_BROKERS)'
    )
    parser.add_argument(
        '--topic',
        type=str,
        default=os.getenv('KAFKA_TOPIC', 'transactions'),
        help='Название топика Kafka (по умолчанию: transactions или из переменной окружения KAFKA_TOPIC)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Размер батча для отправки (по умолчанию: 100)'
    )
    
    args = parser.parse_args()
    
    success = load_csv_to_kafka(
        args.csv_path,
        args.kafka_brokers,
        args.topic,
        args.batch_size
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

