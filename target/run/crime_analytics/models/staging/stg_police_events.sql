
  
    

        create or replace transient table crime_db.staging_staging.stg_police_events
         as
        (

SELECT
    event_id,
    name,
    name AS description,
    type,
    location,
    latitude,
    longitude,
    datetime::TIMESTAMP_NTZ AS event_datetime,
    affected_area,
    api_response,
    loaded_at,
    CURRENT_TIMESTAMP() AS dbt_loaded_at
FROM crime_db.PUBLIC.police_events_staging
WHERE event_id IS NOT NULL
        );
      
  