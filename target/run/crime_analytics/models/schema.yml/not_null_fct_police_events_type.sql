select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select type
from crime_db.staging_mart.fct_police_events
where type is null



      
    ) dbt_internal_test