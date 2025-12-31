SELECT *
FROM {{ ref('mart_merchant_analytics') }}
WHERE is_suspicious NOT IN (0, 1)

