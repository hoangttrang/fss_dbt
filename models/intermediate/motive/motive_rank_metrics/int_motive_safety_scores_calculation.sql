{% set month_str = get_current_month_str() %}

WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, event_breakdown AS (
    SELECT * FROM {{ ref('int_motive_event_breakdown') }}
)

, rank_trips AS (
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, drive_distances AS (SELECT 
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
)

, combined_dd_and_eb AS ( 
    SELECT 
    {%- for month in var('months_list') %}
        dd.{{month | lower}}_miles_driven,
    {% endfor %}
	eb.*
	FROM  drive_distances dd
	LEFT JOIN event_breakdown eb
	ON eb.region = dd.region AND eb.translated_site = dd.translated_site
)

SELECT 
* 
FROM combined_dd_and_eb
WHERE translated_site IS NOT NULL

