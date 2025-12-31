






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and transaction_count >= 0
)
 as expression


    from `fraud_detection`.`mart_daily_state_metrics`
    

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







