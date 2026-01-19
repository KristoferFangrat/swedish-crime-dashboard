-- Crime Analytics Queries
-- Connect to crime_db database and use mart schema

-- 1. Total events and basic statistics
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT type) as unique_event_types,
    COUNT(DISTINCT location) as unique_locations,
    MIN(event_datetime) as earliest_event,
    MAX(event_datetime) as latest_event
FROM fct_police_events;

-- 2. Top 15 event types
SELECT 
    type,
    COUNT(*) as event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM fct_police_events
WHERE type IS NOT NULL
GROUP BY type
ORDER BY event_count DESC
LIMIT 15;

-- 3. Hourly distribution (when do crimes happen?)
SELECT 
    event_hour,
    COUNT(*) as event_count,
    ROUND(AVG(event_count) OVER(), 2) as avg_per_hour
FROM (
    SELECT event_hour, COUNT(*) as event_count 
    FROM fct_police_events 
    WHERE event_hour IS NOT NULL 
    GROUP BY event_hour
)
ORDER BY event_hour;

-- 4. Daily pattern
SELECT 
    CASE day_of_week
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_name,
    COUNT(*) as event_count
FROM fct_police_events
WHERE day_of_week IS NOT NULL
GROUP BY day_of_week
ORDER BY day_of_week;

-- 5. Top 20 locations with most incidents
SELECT 
    location,
    COUNT(*) as event_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM fct_police_events
WHERE location IS NOT NULL AND location != ''
GROUP BY location
ORDER BY event_count DESC
LIMIT 20;

-- 6. Events with GPS coordinates by type
SELECT 
    type,
    COUNT(*) as total_events,
    SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) as with_coordinates,
    ROUND(100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as coverage_percent
FROM fct_police_events
WHERE type IS NOT NULL
GROUP BY type
ORDER BY coverage_percent DESC;

-- 7. Events on specific dates (for trend analysis)
SELECT 
    event_date,
    COUNT(*) as event_count,
    COUNT(DISTINCT type) as unique_types,
    COUNT(DISTINCT location) as unique_locations
FROM fct_police_events
WHERE event_date IS NOT NULL
GROUP BY event_date
ORDER BY event_date DESC
LIMIT 7;

-- 8. Busiest hours by location (Top 5 locations)
WITH top_locations AS (
    SELECT location
    FROM fct_police_events
    WHERE location IS NOT NULL AND location != ''
    GROUP BY location
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT 
    f.location,
    f.event_hour,
    COUNT(*) as event_count
FROM fct_police_events f
INNER JOIN top_locations tl ON f.location = tl.location
WHERE f.event_hour IS NOT NULL
GROUP BY f.location, f.event_hour
ORDER BY f.location, f.event_hour;

-- 9. Event type distribution pie chart data
SELECT 
    type,
    COUNT(*) as count
FROM fct_police_events
WHERE type IS NOT NULL
GROUP BY type
ORDER BY count DESC;

-- 10. Geographic heat - top event types by region
SELECT 
    SUBSTRING(location, 1, 20) as location_prefix,
    type,
    COUNT(*) as event_count
FROM fct_police_events
WHERE location IS NOT NULL AND location != '' AND type IS NOT NULL
GROUP BY location_prefix, type
HAVING COUNT(*) >= 5
ORDER BY event_count DESC
LIMIT 30;

SELECT 
    COUNT(*) as total_events,
    COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as events_with_coords,
    COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as events_without_coords
FROM crime_db.staging_mart.fct_police_events;