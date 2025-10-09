/*
This view will be used for Safety Dashboard reporting 

We want to make sure we using the vehicle group_id and vehicle_group_name from vehicle_group_map and not from combined data events 
*/
WITH combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT
    combined_events.id,
    combined_events.event_id,
    combined_events."type",
    combined_events.driver_id,
    combined_events.driver_first_name,
    combined_events.driver_last_name,
    combined_events.vehicle_id,
    combined_events.coaching_status,
    combined_events.start_date,
    combined_events.severity,
    COALESCE(vehicle_map_rs.group_id, combined_events.group_id) AS group_id,
    COALESCE(vehicle_map_rs.group_name, combined_events.group_name) AS group_name,
    combined_events."month",
    combined_events.created_at,
    combined_events.updated_at,
    combined_events.max_over_speed_in_kph,
    combined_events.max_over_speed_in_mph,
    combined_events.not_current
FROM combined_events
JOIN vehicle_map_rs
    ON combined_events.vehicle_id = vehicle_map_rs.vehicle_id
WHERE 1=1
    AND combined_events.type NOT IN ('drowsiness', 'forward_collision_warning')
    AND combined_events.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
    AND vehicle_map_rs.translated_site IS NOT NULL