
    
    

with all_values as (

    select
        amount_bucket as value_field,
        count(*) as n_records

    from `fraud_detection`.`stg_transactions`
    group by amount_bucket

)

select *
from all_values
where value_field not in (
    'SMALL','MEDIUM','LARGE','VERY_LARGE'
)


