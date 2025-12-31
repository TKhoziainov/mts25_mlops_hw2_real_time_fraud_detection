
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_time
from `fraud_detection`.`stg_transactions`
where transaction_time is null



  
  
    ) dbt_internal_test