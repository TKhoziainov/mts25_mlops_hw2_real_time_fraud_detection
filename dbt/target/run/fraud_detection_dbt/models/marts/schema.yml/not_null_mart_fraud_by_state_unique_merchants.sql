
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_merchants
from `fraud_detection`.`mart_fraud_by_state`
where unique_merchants is null



  
  
    ) dbt_internal_test