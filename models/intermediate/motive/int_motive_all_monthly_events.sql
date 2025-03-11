
WITH combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site
    , combined_events.*

FROM combined_events
JOIN vehicle_map_rs
    ON combined_events.vehicle_id = vehicle_map_rs.vehicle_id
WHERE 1=1
    AND combined_events.type NOT IN ('drowsiness', 'forward_collision_warning')
    AND combined_events.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
    AND vehicle_map_rs.translated_site IS NOT NULL
