
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select risk_level
from `fraud_detection`.`mart_customer_risk_profile`
where risk_level is null



  
  
    ) dbt_internal_test