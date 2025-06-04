WITH vehicle_map_rs AS (
    SELECT * 
    FROM {{ ref('int_motive_vehicle_group_map_rs') }}
    WHERE translated_site IS NOT NULL
)

, driving_period AS (
    SELECT * FROM {{ref('stg_motive_data_driving_periods')}}
)

SELECT 

    dp.driver_id, 
    dp.event_id, 
    dp.driver_first_name, 
    dp.driver_last_name,
    dp.start_date, 
    dp.driving_period_type, 
    month, 
    CASE WHEN ( dp.driver_id is null and dp.driving_period_type !='ym') THEN 1
    ELSE 0 
    END AS unassigned, 
    dp.vehicle_id, 
    v.translated_site AS site,
    v.region,
    v.group_name, 
    CONCAT(v.make, ' - ', v.model) AS car_model
FROM driving_period dp
JOIN vehicle_map_rs v
    ON dp.vehicle_id = v.vehicle_id