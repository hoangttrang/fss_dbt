WITH motive_ukg_labor AS ( 
    SELECT * FROM {{ ref('int_motive_ukg_labor') }}
)

, driving_period AS (
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, get_driver_name AS ( 
    SELECT 
        DISTINCT driver_id, 
        driver_first_name, 
        driver_last_name 
    FROM driving_period
)

, generate_hourly_events AS ( 
    SELECT
        DISTINCT
        group_name, 
        translated_site,
        region,
        event_id,
        vehicle_id,
        event_type, 
        driver_id,
        ukg_id, 
        employment_id,
        start_date_local,
        end_date_local,
        EXTRACT(EPOCH FROM (end_date_local - start_date_local)) / 3600.0 AS trip_duration_hours,
        driving_distance,
        generate_series(
            date_trunc('hour', start_date_local),   -- start from beginning of hour
            date_trunc('hour', end_date_local),     -- go until last hour boundary
            interval '1 hour'
        ) AS hour_bucket
    FROM motive_ukg_labor
)


, get_hourly_event AS (
    SELECT
        group_name,
        translated_site,
        region,
        event_type, 
        event_id, 
        vehicle_id,
        driver_id,
        ukg_id, 
        employment_id,
        driving_distance,
        start_date_local AS start_date_local_ts, 
        end_date_local AS end_date_local_ts,
        CAST(start_date_local AS date) AS start_date_local, 
        CAST(end_date_local AS date) AS end_date_local,
        hour_bucket AS hour_start,
        LEAST(end_date_local, hour_bucket + interval '1 hour')  - GREATEST(start_date_local, hour_bucket) AS duration_interval,
        EXTRACT(EPOCH FROM (end_date_local - start_date_local)) / 3600.0 AS trip_duration_hours,
        EXTRACT(EPOCH FROM (
            LEAST(end_date_local, hour_bucket + interval '1 hour') - GREATEST(start_date_local, hour_bucket))) / 60 AS duration_minutes, 
        EXTRACT(EPOCH FROM (
            LEAST(end_date_local, hour_bucket + interval '1 hour') - GREATEST(start_date_local, hour_bucket))) / 3600 AS duration_hours 
    FROM generate_hourly_events
    WHERE hour_bucket < end_date_local  -- only keep buckets that overlap
    ORDER BY 
        driver_id, 
        hour_start
)

, agg_hourly_event AS (
        SELECT
        group_name,
        translated_site,
        region,
        driver_id,
        ukg_id, 
        employment_id,
        start_date_local,
        end_date_local,
        hour_start,
        -- Driving
        SUM(CASE WHEN event_type = 'driving' THEN duration_minutes ELSE 0 END) AS driving_minutes,
        SUM(CASE WHEN event_type = 'driving' THEN duration_hours   ELSE 0 END) AS driving_hours,
        -- Yard Move
        SUM(CASE WHEN event_type = 'ym' THEN duration_minutes ELSE 0 END) AS ym_minutes,
        SUM(CASE WHEN event_type = 'ym' THEN duration_hours   ELSE 0 END) AS ym_hours,
        -- Idling
        SUM(CASE WHEN event_type = 'idling' THEN duration_minutes ELSE 0 END) AS idling_minutes,
        SUM(CASE WHEN event_type = 'idling' THEN duration_hours   ELSE 0 END) AS idling_hours, 
        -- Driving distance apportioned to bucket
        SUM(
            CASE WHEN event_type IN ('driving', 'ym') 
                 THEN (duration_hours / NULLIF(trip_duration_hours,0)) * driving_distance 
                 ELSE 0 END
        ) AS driving_distance_bucket
    FROM get_hourly_event
    GROUP BY
        group_name,
        translated_site,
        region,
        driver_id,
        ukg_id, 
        employment_id,
        start_date_local,
        end_date_local,
        hour_start
    ORDER BY
        driver_id, 
        start_date_local,
        end_date_local
)

SELECT agg_hourly_event.*
    , driving_minutes + ym_minutes  AS total_minutes
    , driving_hours + ym_hours      AS total_hours
    , name.driver_first_name
    , name.driver_last_name
FROM agg_hourly_event
LEFT JOIN get_driver_name AS name
    ON agg_hourly_event.driver_id = name.driver_id
WHERE agg_hourly_event.driver_id IS NOT NULL AND name.driver_first_name IS NOT NULL 