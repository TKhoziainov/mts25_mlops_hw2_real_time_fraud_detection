
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from `fraud_detection`.`mart_customer_risk_profile`
where customer_id is null



  
  
    ) dbt_internal_test