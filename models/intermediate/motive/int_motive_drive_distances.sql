-- this model 
WITH rank_trips AS (
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site
    {% for month in var('months_list') %}
    , SUM(CASE WHEN LOWER(rank_trips.month) = LOWER('{{month}}') THEN rank_trips.driving_distance ELSE 0 END) AS {{month | lower}}_miles_driven
    {% endfor %}
FROM rank_trips 
JOIN vehicle_map_rs
    ON rank_trips.vehicle_id = vehicle_map_rs.vehicle_id
WHERE rank_trips.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
    AND rank_trips.driving_distance > 0
    AND rank_trips.driving_distance <= 500
    AND rank_trips.row_num = 1
GROUP BY 
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site