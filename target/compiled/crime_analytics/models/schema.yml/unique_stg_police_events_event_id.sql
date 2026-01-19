
    
    

select
    event_id as unique_field,
    count(*) as n_records

from crime_db.staging_staging.stg_police_events
where event_id is not null
group by event_id
having count(*) > 1


