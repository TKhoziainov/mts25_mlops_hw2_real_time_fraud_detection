USE fraud_detection;

SELECT
    us_state,
    cat_id,
FROM (
    SELECT
        us_state,
        cat_id,
        amount,
        transaction_id,
        transaction_time,
        ROW_NUMBER() OVER (PARTITION BY us_state ORDER BY amount DESC) as rn
    FROM transactions
    WHERE us_state != '' AND amount IS NOT NULL
)
WHERE rn = 1
ORDER BY us_state;

