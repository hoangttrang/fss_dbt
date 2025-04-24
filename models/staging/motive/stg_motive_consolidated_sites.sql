WITH data_combined_events AS (
    SELECT * FROM  {{ source ('citus_motive', 'consolidated_sites')}}
)

SELECT 
    id 
    , ukg_location_id
    , motive_group_id
    , motive_group_name
    , consolidated_site 
    , region 
FROM data_combined_events