WITH motive_ukg_labor AS (
    SELECT * FROM {{ ref('int_motive_ukg_labor') }}
)

, driving_period AS (
    SELECT * FROM {{ ref('stg_motive_data_driving_periods') }}
)

, get_driver_name AS ( 
    SELECT 
        DISTINCT driver_id, 
        driver_first_name, 
        driver_last_name 
    FROM driving_period
)

, agg_event_info AS (SELECT
    group_name,
    translated_site,
    region,
    driver_id,
    CAST(start_date AS DATE) AS event_date,
    ukg_id, 
    employment_id,
    SUM(CASE WHEN event_type = 'driving' THEN driving_distance ELSE 0 END) AS driving_distance_driving,
    SUM(CASE WHEN event_type = 'ym'      THEN driving_distance ELSE 0 END) AS driving_distance_ym,
    SUM(CASE WHEN event_type = 'idling'  THEN minutes_per_event ELSE 0 END) AS minutes_idling,
    SUM(CASE WHEN event_type = 'driving' THEN minutes_per_event ELSE 0 END) AS minutes_driving,
    SUM(CASE WHEN event_type = 'ym'      THEN minutes_per_event ELSE 0 END) AS minutes_ym,
    SUM(minutes_per_event)/60 AS total_hours
FROM motive_ukg_labor
WHERE translated_site IS NOT NULL
GROUP BY group_name, 
    translated_site,   
    region, 
    driver_id, 
    CAST(start_date AS DATE),
    ukg_id,
    employment_id) 

, joined_name AS (
    SELECT 
        a.*,
        b.driver_first_name,
        b.driver_last_name
    FROM agg_event_info AS a
    LEFT JOIN get_driver_name AS b
        ON a.driver_id = b.driver_id
)

SELECT * 
FROM joined_name