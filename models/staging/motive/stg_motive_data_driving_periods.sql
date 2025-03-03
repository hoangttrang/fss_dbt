WITH data_driving_periods AS (
    SELECT * FROM  {{ source ('citus_motive', 'data_driving_periods')}}
)

SELECT 

     id
    , event_id
    , driver_id
    , driver_first_name
    , driver_last_name
    , vehicle_id
    , start_date
    , end_date
    , driving_distance
    , driving_period_type
    , driver_company_id
    , minutes_driving
    , CASE 
        WHEN lower(month) = 'january' THEN 1
        WHEN lower(month) = 'february' THEN 2
        WHEN lower(month) = 'march' THEN 3
        WHEN lower(month) = 'april' THEN 4
        WHEN lower(month) = 'may' THEN 5
        WHEN lower(month) = 'june' THEN 6
        WHEN lower(month) = 'july' THEN 7
        WHEN lower(month) = 'august' THEN 8
        WHEN lower(month) = 'september' THEN 9
        WHEN lower(month) = 'october' THEN 10
        WHEN lower(month) = 'november' THEN 11
        WHEN lower(month) = 'december' THEN 12
        ELSE NULL
    END AS month_int,
    , month
    , created_at   
    , updated_at 
    , unassigned
    , not_current 
FROM data_driving_periods;
