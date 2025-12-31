WITH source AS (
    SELECT 
        t.*,
        COALESCE(CAST(s.fraud_flag AS UInt8), 0) AS target
    FROM `fraud_detection`.`transactions` AS t
    LEFT JOIN (
        SELECT 
            transaction_id,
            fraud_flag
        FROM `fraud_detection`.`scores` FINAL
    ) AS s
        ON t.transaction_id = s.transaction_id
),

cleaned AS (
    SELECT
        transaction_id,
        CAST(transaction_time AS DateTime) AS transaction_time,
        CAST(merch AS String) AS merchant_id,
        CAST(cat_id AS String) AS category_id,
        CAST(amount AS Float64) AS amount,
        CAST(name_1 AS String) AS customer_first_name,
        CAST(name_2 AS String) AS customer_last_name,
        CAST(gender AS String) AS gender,
        CAST(us_state AS String) AS us_state,
        CAST(lat AS Float64) AS customer_latitude,
        CAST(lon AS Float64) AS customer_longitude,
        CAST(merchant_lat AS Float64) AS merchant_latitude,
        CAST(merchant_lon AS Float64) AS merchant_longitude,
        COALESCE(CAST(target AS UInt8), 0) AS is_fraud,
        
  CASE
    WHEN amount < 10 THEN 'SMALL'
    WHEN amount < 50 THEN 'MEDIUM'
    WHEN amount < 200 THEN 'LARGE'
    ELSE 'VERY_LARGE'
  END
 AS amount_bucket,
        toDate(transaction_time) AS transaction_date,
        toHour(transaction_time) AS transaction_hour,
        toDayOfWeek(transaction_time) AS day_of_week,
        greatCircleDistance(
            lat * pi() / 180.0,
            lon * pi() / 180.0,
            merchant_lat * pi() / 180.0,
            merchant_lon * pi() / 180.0
        ) / 1609.34 AS distance_miles
    FROM source
    WHERE amount IS NOT NULL
      AND amount >= 0
      AND transaction_time IS NOT NULL
      AND transaction_id IS NOT NULL
)

SELECT * FROM cleaned