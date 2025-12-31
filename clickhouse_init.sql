CREATE DATABASE IF NOT EXISTS fraud_detection;

USE fraud_detection;

CREATE TABLE IF NOT EXISTS transactions_kafka
(
    message String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS transactions
(
    transaction_id String,
    transaction_time DateTime,
    merch String,
    cat_id String,
    amount Float64,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city Int32,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (us_state, transaction_time)
PARTITION BY toYYYYMM(transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
SELECT
    JSONExtractString(message, 'transaction_id') as transaction_id,
    parseDateTimeBestEffort(JSONExtractString(JSONExtractRaw(message, 'data'), 'transaction_time')) as transaction_time,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'merch') as merch,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'cat_id') as cat_id,
    toFloat64OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'amount')) as amount,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'name_1') as name_1,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'name_2') as name_2,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'gender') as gender,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'street') as street,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'one_city') as one_city,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'us_state') as us_state,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'post_code') as post_code,
    toFloat64OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'lat')) as lat,
    toFloat64OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'lon')) as lon,
    toInt32OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'population_city')) as population_city,
    JSONExtractString(JSONExtractRaw(message, 'data'), 'jobs') as jobs,
    toFloat64OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'merchant_lat')) as merchant_lat,
    toFloat64OrNull(JSONExtractString(JSONExtractRaw(message, 'data'), 'merchant_lon')) as merchant_lon
FROM transactions_kafka;

