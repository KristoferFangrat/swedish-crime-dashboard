select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select name
from crime_db.PUBLIC.police_events_staging
where name is null



      
    ) dbt_internal_test