{% set month_str = get_current_month_str() %}


WITH monthly_events AS (
    SELECT * FROM {{ ref('int_motive_all_monthly_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, event_pending_base_grid AS (
    SELECT
        DISTINCT
        vm.region AS "Region",
        vm.translated_site AS "Location",
        'Events Pending Review' AS "Metric"
    FROM vehicle_map_rs vm
    WHERE translated_site is NOT NULL
)

, ranked_events_pending_review AS (
    SELECT 
        region as "Region"
        , translated_site as "Location"
        , 'Events Pending Review' as "Metric"
        {%- for month in var('months_list') -%}
        , COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN event_id END) AS "{{ month }}"
        {%- endfor %}
    FROM monthly_events 
    WHERE 1=1 AND coaching_status = 'pending_review'
    GROUP BY region, translated_site 
)

, final_events_pending_review AS (
    SELECT 
        bg."Region",
        bg."Location",
        bg."Metric"
        {% for month in var('months_list') %}
        , COALESCE(dc."{{ month  }}", 0) AS "{{ month  }}"
         {% endfor %}
    FROM event_pending_base_grid bg
    LEFT JOIN ranked_events_pending_review dc
        ON bg."Region" = dc."Region"
        AND bg."Location" = dc."Location"
        AND bg."Metric" = dc."Metric"
)

SELECT *,
    RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
    RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
FROM final_events_pending_review

