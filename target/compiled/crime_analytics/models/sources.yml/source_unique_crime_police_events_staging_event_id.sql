
    
    

select
    event_id as unique_field,
    count(*) as n_records

from crime_db.PUBLIC.police_events_staging
where event_id is not null
group by event_id
having count(*) > 1


