WITH customer_stats AS (
    SELECT
        CONCAT(customer_first_name, '_', customer_last_name) AS customer_id,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS Float64) / COUNT(*) * 100 AS fraud_rate,
        AVG(amount) AS avg_amount,
        SUM(amount) AS total_amount,
        MIN(transaction_time) AS first_transaction,
        MAX(transaction_time) AS last_transaction
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
),

customer_risk AS (
    SELECT
        customer_id,
        transaction_count,
        fraud_count,
        fraud_rate,
        avg_amount,
        total_amount,
        first_transaction,
        last_transaction,
        CASE
            WHEN fraud_rate >= 50 OR (fraud_count >= 3 AND fraud_rate >= 20) THEN 'HIGH'
            WHEN fraud_rate >= 10 OR fraud_count >= 2 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level
    FROM customer_stats
)

SELECT * FROM customer_risk
ORDER BY risk_level DESC, fraud_rate DESC

