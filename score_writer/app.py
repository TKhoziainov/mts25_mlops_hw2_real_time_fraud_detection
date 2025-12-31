import os
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def insert_scores(conn, scores_data):
    try:
        with conn.cursor() as cur:
            for record in scores_data:
                transaction_id = record.get('transaction_id')
                score = record.get('score')
                fraud_flag = record.get('fraud_flag')
                
                if transaction_id and score is not None and fraud_flag is not None:
                    cur.execute(
                        """
                        INSERT INTO fraud_scores (transaction_id, score, fraud_flag)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_id) 
                        DO UPDATE SET 
                            score = EXCLUDED.score,
                            fraud_flag = EXCLUDED.fraud_flag,
                            created_at = CURRENT_TIMESTAMP
                        """,
                        (transaction_id, float(score), int(fraud_flag))
                    )
            conn.commit()
            logger.info(f"Inserted {len(scores_data)} records into database")
    except Exception as e:
        logger.error(f"Error inserting scores: {e}")
        conn.rollback()
        raise


def main():
    logger.info('Starting score writer service...')
    
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'score-writer',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_SCORES_TOPIC])
    
    logger.info(f'Subscribed to topic: {KAFKA_SCORES_TOPIC}')
    
    db_conn = None
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            db_conn = get_db_connection()
            logger.info('Connected to PostgreSQL database')
            break
        except Exception as e:
            retry_count += 1
            logger.warning(f"Failed to connect to database (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                import time
                time.sleep(5)
            else:
                logger.error("Failed to connect to database after multiple attempts")
                return
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                
                if isinstance(data, list):
                    scores_data = data
                else:
                    scores_data = [data]
                
                insert_scores(db_conn, scores_data)
                
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        consumer.close()
        if db_conn:
            db_conn.close()
        logger.info('Score writer service stopped')


if __name__ == "__main__":
    main()

