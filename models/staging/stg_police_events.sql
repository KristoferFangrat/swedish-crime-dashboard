{{
    config(
        materialized='table',
        schema='staging'
    )
}}

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
FROM {{ source('crime', 'police_events_staging') }}
WHERE event_id IS NOT NULL
