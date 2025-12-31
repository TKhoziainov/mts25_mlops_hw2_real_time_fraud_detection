
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_transactions
from `fraud_detection`.`mart_fraud_by_category`
where total_transactions is null



  
  
    ) dbt_internal_test