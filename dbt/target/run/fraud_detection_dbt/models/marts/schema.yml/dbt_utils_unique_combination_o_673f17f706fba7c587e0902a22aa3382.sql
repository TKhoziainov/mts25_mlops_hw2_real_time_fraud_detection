
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        transaction_hour, day_of_week
    from `fraud_detection`.`mart_hourly_fraud_pattern`
    group by transaction_hour, day_of_week
    having count(*) > 1

)

select *
from validation_errors



  
  
    ) dbt_internal_test