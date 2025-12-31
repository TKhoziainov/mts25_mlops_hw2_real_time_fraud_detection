
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fraud_rate
from `fraud_detection`.`mart_hourly_fraud_pattern`
where fraud_rate is null



  
  
    ) dbt_internal_test