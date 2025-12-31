
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT *
FROM `fraud_detection`.`mart_merchant_analytics`
WHERE is_suspicious NOT IN (0, 1)
  
  
    ) dbt_internal_test