WITH driving_period AS (
    SELECT * FROM {{ ref('stg_motive_data_driving_periods') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
    a.event_id
    , a.driver_id
    , a.driver_first_name
    , a.driver_last_name
    , a.vehicle_id
    , a.start_date
    , a.end_date
    , a.driving_distance
    , a.driving_period_type
    , a.driver_company_id
    ,a.minutes_driving
    , a.month
    , a.created_at
    , a.updated_at
    , a.unassigned
    , vehicle_map_rs.number
    , vehicle_map_rs.status
    , vehicle_map_rs.make
    , vehicle_map_rs.model
    , vehicle_map_rs.group_name
    , vehicle_map_rs.translated_site
    , vehicle_map_rs.region
    , ROW_NUMBER() OVER (
        PARTITION BY a.vehicle_id, a.start_date
        ORDER BY CASE WHEN driver_id IS NOT NULL THEN 1 ELSE 2 END
    ) AS row_num
FROM  driving_period AS a
JOIN vehicle_map_rs
    on a.vehicle_id = vehicle_map_rs.vehicle_id
WHERE 1=1
AND a.start_date BETWEEN  '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'