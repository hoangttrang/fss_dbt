
{% set month_str = get_current_month_str() %}


WITH monthly_events AS (
    SELECT * FROM {{ ref('int_motive_all_monthly_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, rank_trips AS ( 
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, unassigned_base_grid AS (
    SELECT
        DISTINCT
        vm.region AS "Region",
        vm.translated_site AS "Location",
        'Percent Unassigned Trips' AS "Metric"
    FROM vehicle_map_rs vm
    WHERE translated_site is NOT NULL
)

, pct_unassigned_ranked AS (
    SELECT
        region AS "Region"
        , translated_site AS "Location"
        , 'Percent Unassigned Trips' AS "Metric"
        {%- for month in var('months_list') -%}
        , COALESCE(CAST(COUNT(DISTINCT CASE WHEN driver_id IS NULL AND month = '{{ month }}' THEN event_id END) AS FLOAT) 
        / NULLIF(COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN event_id END), 0), 0) AS "{{ month }}"
        {%- endfor %}
    FROM rank_trips
    WHERE 1=1
    AND row_num = 1
    GROUP BY region, translated_site, "Metric"
)
, pct_unassigned_final AS (
    SELECT 
        bg."Region",
        bg."Location",
        bg."Metric"
        {% for month in var('months_list') %}
        , COALESCE(dc."{{ month }}", 0) AS "{{ month  }}"
         {% endfor %}
    FROM unassigned_base_grid bg
    LEFT JOIN pct_unassigned_ranked dc
        ON bg."Region" = dc."Region"
        AND bg."Location" = dc."Location"
        AND bg."Metric" = dc."Metric"
)


SELECT *
FROM pct_unassigned_final


