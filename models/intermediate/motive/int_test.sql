WITH combined_events AS (
    SELECT * FROM {{ ref('stg_motive_data_combined_events') }}
)
, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
    vehicle_map_rs.region,
    vehicle_map_rs.translated_site

{% for month in var('months_list') %}
  {% for event_type in get_motive_event_type() %}

    {% if event_type|lower == 'speeding' %}
      -- Speeding 0–5 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND combined_events.max_over_speed_in_mph BETWEEN 0 AND 5
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding

      -- Speeding 5–10 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND combined_events.max_over_speed_in_mph BETWEEN 5 AND 10
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_five

      -- Speeding 6–10 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND combined_events.max_over_speed_in_mph BETWEEN 6 AND 10
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_six_to_ten

      -- Speeding 10–15 mph
      , SUM(
          CASE 
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
                 AND combined_events.max_over_speed_in_mph BETWEEN 10 AND 15
            THEN 1 
            ELSE 0 
          END
        ) AS {{ month|lower }}_speeding_ten_to_fifteen

    {% else %}
      -- For all other event types, just do a single column
      , SUM(
          CASE
            WHEN LOWER(combined_events.month) = LOWER('{{ month }}')
                 AND combined_events.type = '{{ event_type }}'
            THEN 1
            ELSE 0
          END
        ) AS {{ month|lower }}_{{ event_type }}
    {% endif %}

  {% endfor %}
{% endfor %}

FROM combined_events
JOIN vehicle_map_rs
    ON combined_events.vehicle_id = vehicle_map_rs.vehicle_id
GROUP BY 
    vehicle_map_rs.region,
    vehicle_map_rs.translated_site
