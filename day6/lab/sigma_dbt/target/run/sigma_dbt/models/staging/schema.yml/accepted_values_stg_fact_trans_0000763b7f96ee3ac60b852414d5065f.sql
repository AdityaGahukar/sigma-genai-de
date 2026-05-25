
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        payment_method as value_field,
        count(*) as n_records

    from SIGMA_DE.PUBLIC.stg_fact_transactions
    group by payment_method

)

select *
from all_values
where value_field not in (
    'CREDIT_CARD','DEBIT_CARD','UPI'
)



  
  
      
    ) dbt_internal_test