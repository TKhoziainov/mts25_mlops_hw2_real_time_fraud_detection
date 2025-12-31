






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and amount >= 0 and amount <= 1000000
)
 as expression


    from `fraud_detection`.`stg_transactions`
    

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







