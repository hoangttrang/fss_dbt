WITH data_inspect AS (
    SELECT * FROM  {{ source ('citus_motive', 'data_inspections')}}
) 

SELECT 
    , CAST(id AS INT) AS id
    , CAST(inspection_id AS INT) AS inspection_id
    , CAST(vehicle_id AS INT) AS vehicle_id
    , CAST(date AS DATE) AS date
    , CAST(location AS VARCHAR(255)) AS location
    , CAST(status AS VARCHAR(50)) AS status
    , CAST(inspection_type AS VARCHAR(50)) AS inspection_type
    , CAST(driver_id AS INT) AS driver_id
    , CAST(mechanic_id AS INT) AS mechanic_id
    , CAST(reviewer_id AS INT) AS reviewer_id
FROM data_inspect
