

SELECT
    event_id,
    name,
    description,
    type,
    location,
    latitude,
    longitude,
    event_datetime,
    affected_area,
    DATE(event_datetime) AS event_date,
    EXTRACT(HOUR FROM event_datetime) AS event_hour,
    EXTRACT(DAYOFWEEK FROM event_datetime) AS day_of_week,
    dbt_loaded_at
FROM crime_db.staging_staging.stg_police_events