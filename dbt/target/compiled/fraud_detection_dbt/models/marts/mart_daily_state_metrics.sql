WITH daily_transactions AS (
    SELECT
        transaction_date,
        us_state,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        quantile(0.95)(amount) AS p95_amount,
        SUM(CASE WHEN amount_bucket IN ('LARGE', 'VERY_LARGE') THEN 1 ELSE 0 END) AS large_transaction_count
    FROM `fraud_detection`.`stg_transactions`
    GROUP BY transaction_date, us_state
)

SELECT
    transaction_date,
    us_state,
    transaction_count,
    total_amount,
    avg_amount,
    p95_amount,
    large_transaction_count,
    CAST(large_transaction_count AS Float64) / transaction_count * 100 AS large_transaction_pct
FROM daily_transactions
ORDER BY transaction_date DESC, us_state