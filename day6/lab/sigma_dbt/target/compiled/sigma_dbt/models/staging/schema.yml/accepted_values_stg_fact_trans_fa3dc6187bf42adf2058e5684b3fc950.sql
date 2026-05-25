
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from SIGMA_DE.PUBLIC.stg_fact_transactions
    group by status

)

select *
from all_values
where value_field not in (
    'COMPLETED','FAILED','PENDING'
)


