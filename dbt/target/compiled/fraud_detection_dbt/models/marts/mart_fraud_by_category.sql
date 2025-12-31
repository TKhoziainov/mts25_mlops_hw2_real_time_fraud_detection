WITH category_stats AS (
    SELECT
        category_id,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount,
        AVG(CASE WHEN is_fraud = 1 THEN amount ELSE NULL END) AS avg_fraud_amount
    FROM `fraud_detection`.`stg_transactions`
    GROUP BY category_id
)

SELECT
    category_id,
    total_transactions,
    fraud_count,
    CAST(fraud_count AS Float64) / total_transactions * 100 AS fraud_rate,
    total_amount,
    fraud_amount,
    avg_fraud_amount
FROM category_stats
ORDER BY fraud_rate DESC, total_transactions DESC