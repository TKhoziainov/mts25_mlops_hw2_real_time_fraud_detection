
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  with relation_columns as (

        
        select
            cast('TRANSACTION_ID' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('TRANSACTION_TIME' as String) as relation_column,
            cast('DATETIME' as String) as relation_column_type
        union all
        
        select
            cast('MERCH' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('CAT_ID' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('AMOUNT' as String) as relation_column,
            cast('FLOAT64' as String) as relation_column_type
        union all
        
        select
            cast('NAME_1' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('NAME_2' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('GENDER' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('STREET' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('ONE_CITY' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('US_STATE' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('POST_CODE' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('LAT' as String) as relation_column,
            cast('FLOAT64' as String) as relation_column_type
        union all
        
        select
            cast('LON' as String) as relation_column,
            cast('FLOAT64' as String) as relation_column_type
        union all
        
        select
            cast('POPULATION_CITY' as String) as relation_column,
            cast('INT32' as String) as relation_column_type
        union all
        
        select
            cast('JOBS' as String) as relation_column,
            cast('STRING' as String) as relation_column_type
        union all
        
        select
            cast('MERCHANT_LAT' as String) as relation_column,
            cast('FLOAT64' as String) as relation_column_type
        union all
        
        select
            cast('MERCHANT_LON' as String) as relation_column,
            cast('FLOAT64' as String) as relation_column_type
        union all
        
        select
            cast('CREATED_AT' as String) as relation_column,
            cast('DATETIME' as String) as relation_column_type
        
        
    ),
    test_data as (

        select
            *
        from
            relation_columns
        where
            relation_column = 'AMOUNT'
            and
            relation_column_type not in ('FLOAT64')

    )
    select *
    from test_data
  
  
    ) dbt_internal_test