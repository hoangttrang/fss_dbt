WITH combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
	vehicle_map_rs.region
    , vehicle_map_rs.translated_site
{%- for month in var('months_list') %}
  {% for event_type in get_motive_event_type() -%}

    {%- if event_type|lower == 'speeding' %}
      -- Speeding 0–5 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND (combined_events.max_over_speed_in_mph < 5)
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding

      -- Speeding 5–10 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND (combined_events.max_over_speed_in_mph >= 5 AND combined_events.max_over_speed_in_mph < 6)
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_five

      -- Speeding 6–10 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND (combined_events.max_over_speed_in_mph >= 6 AND combined_events.max_over_speed_in_mph < 10)
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_six_to_ten

      -- Speeding 10–15 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND (combined_events.max_over_speed_in_mph >= 10 AND combined_events.max_over_speed_in_mph < 15)
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_ten_to_fifteen
      -- Speeding 15+ mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND combined_events.max_over_speed_in_mph >= 15
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_fifteen_plus

    {%- else -%}
      -- For all other event types, just do a single column
      , SUM(
          CASE
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
            THEN 1
            ELSE 0
          END
        ) AS {{ month|lower }}_{{ event_type }}
    {%- endif -%}

  {%- endfor %}
{% endfor %}

FROM combined_events
JOIN vehicle_map_rs
    ON combined_events.vehicle_id = vehicle_map_rs.vehicle_id
WHERE 1=1
    AND combined_events.coaching_status <> 'uncoachable'
    AND combined_events.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
GROUP BY
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site