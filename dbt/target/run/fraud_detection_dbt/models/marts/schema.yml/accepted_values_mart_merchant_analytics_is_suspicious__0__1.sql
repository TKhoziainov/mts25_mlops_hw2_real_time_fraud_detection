
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        is_suspicious as value_field,
        count(*) as n_records

    from `fraud_detection`.`mart_merchant_analytics`
    group by is_suspicious

)

select *
from all_values
where value_field not in (
    '0','1'
)



  
  
    ) dbt_internal_test