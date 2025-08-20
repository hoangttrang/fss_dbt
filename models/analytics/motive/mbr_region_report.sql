{% set month_str = get_current_month_str() %}
{% set prev_month_str = get_previous_month_str() %}


WITH safety_scores_metrics AS (
    SELECT * FROM {{ ref('int_motive_safety_scores_metrics_region') }}
)


, dvir_region_metrics_region AS (
    SELECT * FROM {{ ref('int_motive_dvir_metrics_region') }}
)

, event_pending_review AS (
    SELECT * FROM {{ ref('int_motive_event_pending_review_metrics') }}
)

, pct_unassigned_final_metrics_region AS (
    SELECT * FROM {{ ref('int_motive_event_unassigned_metrics_region') }}
)

, events_moved_to_uncoachable AS (
    SELECT * FROM {{ ref('int_motive_event_uncoachable_metrics') }}
)

, events_per_vehicle_metrics_region AS (
    SELECT * FROM {{ ref('int_motive_events_p_vehicles_metrics_region') }}
)


-- Import miles driven as a metric 
, combined_dd_and_eb AS ( 
    SELECT * 
    FROM {{ ref('int_motive_safety_scores_calculation') }} 
)

, monthly_miles_driven AS ( 
    SELECT
        region AS "Region"
        , 'Miles Driven' AS "Metric"
        {%- for month in var('months_list') %}
        , SUM( {{month}}_miles_driven) AS "{{month}}"
        {%- endfor %}     
    FROM combined_dd_and_eb
    GROUP BY region, "Metric"
)

, region_monthly_miles_driven AS (
    SELECT
        "Region"
        , "Metric"
        {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC) AS "{{month}}"
        {%- endfor %}
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" DESC) AS "Region Rank"
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" DESC) AS "Prev Region Rank"
    FROM monthly_miles_driven
)

, event_pending_review_region_metrics AS (
    SELECT 
        "Region",
        "Metric"
    {%- for month in var('months_list') -%}
        , SUM("{{ month }}") AS "{{ month }}"
    {%- endfor %}
    FROM event_pending_review
    GROUP BY 
        "Region",
        "Metric"
)

, events_moved_to_uncoachable_metrics AS (
    SELECT 
        "Region",
        "Metric"
    {%- for month in var('months_list') -%}
        , SUM("{{ month }}") AS "{{ month }}"
    {%- endfor %}
    FROM events_moved_to_uncoachable
    GROUP BY 
        "Region",
        "Metric"
)

, rounded_safety_scores AS (
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM safety_scores_metrics
    
)

-- Give Region Ranking for metrics 
, safety_scores_rank_region AS (
    SELECT *
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{month_str}}" DESC) AS "Region Rank"
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{prev_month_str}}" DESC) AS "Prev Region Rank"
    FROM rounded_safety_scores    
)

, rounded_dvir_rank AS ( 
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM dvir_region_metrics_region
)

, dvir_rank_region AS ( 
    SELECT *,
        RANK() OVER (PARTITION BY "Metric" ORDER BY "{{month_str}}" DESC) AS "Region Rank"
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{prev_month_str}}" DESC) AS "Prev Region Rank"
    FROM rounded_dvir_rank
    ORDER BY "Metric", "Region"
)

, rounded_events_per_vehicle AS (
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM events_per_vehicle_metrics_region
)

, events_per_vehicle_rank_region AS (
    SELECT *
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_per_vehicle
)

, rounded_events_pending_review AS (
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM event_pending_review_region_metrics
)

, events_pending_review_rank_region AS ( 
    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_pending_review
)

, rounded_pct_unassigned AS (
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM pct_unassigned_final_metrics_region
)

, pct_unassigned_final_rank_region AS ( 
    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_pct_unassigned
)

, rounded_events_moved_to_uncoachable AS (
    SELECT 
        "Region"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM events_moved_to_uncoachable_metrics
)

, events_moved_to_uncoachable_rank_region AS ( 
    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_moved_to_uncoachable
) 

-- Union all metrics with ranking
, union_table AS (
    SELECT * FROM dvir_rank_region
    UNION ALL
    SELECT * FROM events_pending_review_rank_region
    UNION ALL
    SELECT * FROM pct_unassigned_final_rank_region
    UNION ALL
    SELECT * FROM events_moved_to_uncoachable_rank_region
    UNION ALL 
    SELECT * FROM events_per_vehicle_rank_region
    UNION ALL 
    SELECT * FROM safety_scores_rank_region
    UNION ALL 
    SELECT * FROM region_monthly_miles_driven
)

SELECT 
    *
FROM union_table