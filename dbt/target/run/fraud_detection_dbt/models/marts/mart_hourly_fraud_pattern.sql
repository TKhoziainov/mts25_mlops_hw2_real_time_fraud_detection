
  
    
    
    
        
         


        insert into `fraud_detection`.`mart_hourly_fraud_pattern`
        ("transaction_hour", "day_of_week", "day_name", "total_transactions", "fraud_count", "fraud_rate", "avg_amount", "risk_window")WITH hourly_stats AS (
    SELECT
        transaction_hour,
        day_of_week,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        CAST(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS Float64) / COUNT(*) * 100 AS fraud_rate,
        AVG(amount) AS avg_amount
    FROM `fraud_detection`.`stg_transactions`
    GROUP BY transaction_hour, day_of_week
)

SELECT
    transaction_hour,
    day_of_week,
    CASE day_of_week
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
        ELSE 'Unknown'
    END AS day_name,
    total_transactions,
    fraud_count,
    fraud_rate,
    avg_amount,
    CASE
        WHEN fraud_rate > 20 THEN 'HIGH_RISK'
        WHEN fraud_rate > 10 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_window
FROM hourly_stats
ORDER BY fraud_rate DESC, transaction_hour, day_of_week
  