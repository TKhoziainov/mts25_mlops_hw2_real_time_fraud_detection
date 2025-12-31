






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and transaction_hour >= 0 and transaction_hour <= 23
)
 as expression


    from `fraud_detection`.`mart_hourly_fraud_pattern`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







