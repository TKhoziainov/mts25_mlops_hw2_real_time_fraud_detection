





with validation_errors as (

    select
        transaction_hour, day_of_week
    from `fraud_detection`.`mart_hourly_fraud_pattern`
    group by transaction_hour, day_of_week
    having count(*) > 1

)

select *
from validation_errors


