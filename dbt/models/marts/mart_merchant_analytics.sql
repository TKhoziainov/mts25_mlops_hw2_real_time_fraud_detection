WITH merchant_stats AS (
    SELECT
        merchant_id,
        COUNT(*) AS transaction_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS Float64) / COUNT(*) * 100 AS fraud_rate,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_transaction_amount,
        COUNT(DISTINCT us_state) AS states_count,
        COUNT(DISTINCT category_id) AS categories_count
    FROM {{ ref('stg_transactions') }}
    GROUP BY merchant_id
)

SELECT
    merchant_id,
    transaction_count,
    fraud_count,
    fraud_rate,
    total_revenue,
    avg_transaction_amount,
    states_count,
    categories_count,
    CASE
        WHEN fraud_rate > 15 OR (fraud_count >= 5 AND fraud_rate > 10) THEN 1
        ELSE 0
    END AS is_suspicious
FROM merchant_stats
ORDER BY fraud_rate DESC, total_revenue DESC

