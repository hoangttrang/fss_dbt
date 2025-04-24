{% set month_str = get_current_month_str() %}

WITH event_breakdown AS (
    SELECT * FROM {{ ref('int_motive_event_breakdown') }}
)

, rank_trips AS (
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)
, all_monthly_events AS (
    SELECT * FROM {{ ref('int_motive_all_monthly_events') }}
)

-- not account for drowsiness and forward collisions
, monthly_events AS (
    SELECT
        region
        , translated_site
        {%- for month in var('months_list') -%}
        , COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN event_id END) AS events_{{ month | lower }}
        {%- endfor %}
    FROM all_monthly_events
    WHERE 1=1 AND coaching_status <> 'uncoachable'
    GROUP BY region, translated_site
)

, monthly_vehicles AS (
    SELECT 
        region
        , translated_site
    {%- for month in var('months_list') -%}
        , COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN vehicle_id END) AS {{ month | lower }}_vehicles
    {%- endfor %}
    FROM rank_trips 
    WHERE 1=1 and row_num = 1 
    GROUP BY 
        region, 
        translated_site
)

, ranked_events_per_vehicle AS (
    SELECT 
        me.region AS "Region"
       , me.translated_site as "Location"
	   , 'Events Per Vehicle' as "Metric"
    {%- for month in var('months_list') -%}
       , COALESCE(CAST(events_{{ month | lower }} AS FLOAT) / NULLIF({{ month | lower }}_vehicles, 0), 0) AS "{{ month }}"
    {%- endfor %}
    FROM monthly_events me
    JOIN monthly_vehicles mv 
    ON me.region = mv.region AND me.translated_site = mv.translated_site
)

SELECT *
FROM ranked_events_per_vehicle
