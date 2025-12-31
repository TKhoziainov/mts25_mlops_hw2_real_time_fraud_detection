
  
    
    
    
        
         


        insert into `fraud_detection`.`mart_fraud_by_state`
        ("us_state", "total_transactions", "fraud_count", "fraud_rate", "unique_customers", "unique_merchants", "total_amount", "fraud_amount")WITH state_stats AS (
    SELECT
        us_state,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS Float64) / COUNT(*) * 100 AS fraud_rate,
        COUNT(DISTINCT CONCAT(customer_first_name, '_', customer_last_name)) AS unique_customers,
        COUNT(DISTINCT merchant_id) AS unique_merchants,
        SUM(amount) AS total_amount,
        SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END) AS fraud_amount
    FROM `fraud_detection`.`stg_transactions`
    GROUP BY us_state
)

SELECT
    us_state,
    total_transactions,
    fraud_count,
    fraud_rate,
    unique_customers,
    unique_merchants,
    total_amount,
    fraud_amount
FROM state_stats
ORDER BY fraud_rate DESC, total_transactions DESC
  