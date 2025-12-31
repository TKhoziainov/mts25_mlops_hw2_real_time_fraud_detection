





with validation_errors as (

    select
        transaction_date, us_state
    from `fraud_detection`.`mart_daily_state_metrics`
    group by transaction_date, us_state
    having count(*) > 1

)

select *
from validation_errors


