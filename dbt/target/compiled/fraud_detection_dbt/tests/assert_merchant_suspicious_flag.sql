SELECT *
FROM `fraud_detection`.`mart_merchant_analytics`
WHERE is_suspicious NOT IN (0, 1)