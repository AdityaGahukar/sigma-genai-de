
    
    

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


