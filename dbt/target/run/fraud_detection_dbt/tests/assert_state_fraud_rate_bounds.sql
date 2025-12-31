
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  SELECT *
FROM `fraud_detection`.`mart_fraud_by_state`
WHERE fraud_rate > 100 OR fraud_rate < 0
  
  
    ) dbt_internal_test