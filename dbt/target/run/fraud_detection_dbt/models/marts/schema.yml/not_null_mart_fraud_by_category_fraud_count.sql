
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fraud_count
from `fraud_detection`.`mart_fraud_by_category`
where fraud_count is null



  
  
    ) dbt_internal_test