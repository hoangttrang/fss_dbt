WITH combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
	vehicle_map_rs.region,
    vehicle_map_rs.translated_site, 
{% for month in var('months_list') %}
    {% for event_type in var('motive_event_type') %}
    SUM(CASE WHEN LOWER(combined_events.month) = LOWER('{{ month }}') AND combined_events.type = event_type THEN 1 ELSE 0 END) AS {{ month | lower }}_{{ event_type }},
    {% endfor %}
{% endfor %}

FROM combined_events
JOIN vehicle_map_rs
    ON combined_events.vehicle_id = vehicle_map.vehicle_id
WHERE 1=1
    AND combined_events.coaching_status <> 'uncoachable'
    AND combined_events.start_date BETWEEN "{{var('mbr_start_date')}}" AND "{{ var('mbr_report_date')}}"
GROUP BY
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site