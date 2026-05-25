
    
    

with all_values as (

    select
        failure_rate_pct as value_field,
        count(*) as n_records

    from SIGMA_DE.PUBLIC.mart_transaction_summary
    group by failure_rate_pct

)

select *
from all_values
where value_field not in (
    '999'
)


