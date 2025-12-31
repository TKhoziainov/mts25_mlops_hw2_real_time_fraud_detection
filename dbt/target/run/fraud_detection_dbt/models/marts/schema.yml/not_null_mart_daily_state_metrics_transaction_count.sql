
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_count
from `fraud_detection`.`mart_daily_state_metrics`
where transaction_count is null



  
  
    ) dbt_internal_test