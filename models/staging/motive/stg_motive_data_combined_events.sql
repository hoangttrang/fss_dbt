WITH data_combined_events AS (
    SELECT * FROM  {{ source ('citus_motive', 'data_combined_events')}}
)

SELECT 
    id
    , event_id
    , type
    , driver_id
    , driver_first_name
    , driver_last_name
    , vehicle_id
    , start_date
    , coaching_status
    , severity
    , group_id
    , group_name
    , month
    , created_at 
    , updated_at
    , max_over_speed_in_kph
    , max_over_speed_in_mph 
    , not_current 
FROM data_combined_events
WHERE not_current IS FALSE OR not_current IS NULL