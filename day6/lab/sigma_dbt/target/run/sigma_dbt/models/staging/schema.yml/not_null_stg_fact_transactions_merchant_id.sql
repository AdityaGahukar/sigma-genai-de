
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select merchant_id
from SIGMA_DE.PUBLIC.stg_fact_transactions
where merchant_id is null



  
  
      
    ) dbt_internal_test