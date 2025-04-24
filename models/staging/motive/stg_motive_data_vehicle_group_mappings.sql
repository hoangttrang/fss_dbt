WITH data_vehicle_group_mappings AS (
    SELECT * FROM  {{ source ('citus_motive', 'data_vehicle_group_mappings')}}
)

SELECT 
    id
    , vehicle_id   
    , number
    , status
    , make
    , model
    , group_id
    , TRIM(group_name) AS group_name
FROM data_vehicle_group_mappings