
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_suspicious
from `fraud_detection`.`mart_merchant_analytics`
where is_suspicious is null



  
  
    ) dbt_internal_test