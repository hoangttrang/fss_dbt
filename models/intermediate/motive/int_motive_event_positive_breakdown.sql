WITH positive_events AS (
    SELECT *
    , trim(to_char(to_date(EXTRACT(month FROM start_date)::text, 'MM'::text)::timestamp with time zone, 'Month'::text)) AS month
    FROM {{ ref('stg_motive_data_positive_behavior_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
    WHERE translated_site IS NOT NULL
)

SELECT 
	vehicle_map_rs.region
    , vehicle_map_rs.translated_site
{%- for month in var('months_list') %}
  {% for event_type in get_motive_positive_event_type() %}
      , SUM(
          CASE
            WHEN LOWER(positive_events.month) = LOWER('{{ month }}')
                 AND positive_events.positive_behavior = '{{ event_type }}'
            THEN 1
            ELSE 0
          END
        ) AS {{ month|lower }}_{{ event_type }}

  {%- endfor %}
{% endfor %}

FROM positive_events
JOIN vehicle_map_rs
    ON positive_events.vehicle_id = vehicle_map_rs.vehicle_id
WHERE 1=1
    AND positive_events.coaching_status <> 'uncoachable'
    AND positive_events.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
GROUP BY
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site