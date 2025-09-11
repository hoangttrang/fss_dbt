WITH motive_ukg_mapping AS (
    SELECT 
    *, 
    CAST(motive_id AS integer) AS motive_id_int
    FROM {{ ref('stg_motive_ukg_mapping') }}
)

, motive_timezone AS (
    SELECT * FROM {{ ref ('consolidated_site_mapping_with_timezone')}}
)

, driving_period AS (
    SELECT * 
    , ROW_NUMBER() OVER (
        PARTITION BY a.vehicle_id, a.start_date
        ORDER BY CASE WHEN driver_id IS NOT NULL THEN 1 ELSE 2 END
    ) AS row_num
    FROM {{ ref('stg_motive_data_driving_periods') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, data_idle_events AS (
    SELECT * FROM {{ ref('stg_motive_data_idle_events') }}
)

, driving_information AS (
    SELECT 
        a.event_id
        , a.driver_id
        , a.vehicle_id
        , a.start_date
        , a.end_date
        , a.driving_distance
        , a.minutes_driving AS minutes_per_event
        , a.driving_period_type AS event_type
        , vehicle_map_rs.group_name
        , vehicle_map_rs.translated_site
        , vehicle_map_rs.region
    FROM  driving_period AS a
    LEFT JOIN vehicle_map_rs
        on a.vehicle_id = vehicle_map_rs.vehicle_id
    WHERE row_num = 1
        AND a.driver_id IS NOT NULL and a.driving_period_type != 'pc'
        AND a.driving_distance > 0 AND a.driving_distance < 500
) 

, idle_event AS ( 
    SELECT
        idle_event.event_id
        , idle_event.driver_id
        , idle_event.vehicle_id
        , idle_event.start_time
        , idle_event.end_time
        , 0 AS driving_distance
        , idle_event.minutes_idling AS minutes_per_event
        , 'idling' AS event_type
        , vehicle_map_rs.group_name
        , vehicle_map_rs.translated_site
        , vehicle_map_rs.region
    FROM data_idle_events idle_event
    LEFT JOIN vehicle_map_rs
        on idle_event.vehicle_id = vehicle_map_rs.vehicle_id
    WHERE idle_event.minutes_idling > 0
        AND vehicle_map_rs.translated_site IS NOT NULL
)

, union_info AS (
    SELECT * FROM driving_information
    UNION ALL
    SELECT * FROM idle_event
)

, mapped_ukg_info AS (
    SELECT 
        union_info.*
        , ukg.ukg_id
        , ukg.employee_id AS employment_id
    FROM union_info
    LEFT JOIN motive_ukg_mapping ukg
        ON union_info.driver_id = ukg.motive_id_int
)

, joined_timezone AS (
    SELECT 
        mapped_ukg_info.*
        , motive_timezone.timezone
        , start_date AT TIME ZONE motive_timezone.timezone AS start_date_local
        , end_date AT TIME ZONE motive_timezone.timezone AS end_date_local
    FROM mapped_ukg_info AS mapped_ukg_info
    LEFT JOIN motive_timezone AS motive_timezone
        ON mapped_ukg_info.translated_site = motive_timezone.consolidated_site
         AND mapped_ukg_info.region = motive_timezone.region
         AND mapped_ukg_info.group_name = motive_timezone.motive_group_name
    WHERE translated_site IS NOT NULL
)

SELECT 
    *
FROM joined_timezone
