
{% set month_str = get_current_month_str() %}
{% set prev_month_str = get_previous_month_str() %}

WITH safety_scores_metrics AS (
    SELECT * FROM {{ ref('int_motive_safety_scores_metrics_sites') }}
)

, final_events_per_vehicle_metrics AS (
    SELECT * FROM {{ ref('int_motive_events_p_vehicles_metrics_sites') }}
)

, final_events_moved_to_uncoachable_metrics AS ( 
    SELECT * FROM {{ ref('int_motive_event_uncoachable_metrics') }}
)

, final_events_pending_review_metrics AS ( 
    SELECT * FROM {{ ref('int_motive_event_pending_review_metrics') }}
)

, pct_unassigned_final_metrics AS ( 
    SELECT * FROM {{ ref('int_motive_event_unassigned_metrics_sites') }}
)

, dvir_metrics AS ( 
    SELECT * FROM {{ ref('int_motive_dvir_metrics_sites') }}
)

-- Import miles driven as a metric 
, combined_dd_and_eb AS ( 
    SELECT * 
    FROM {{ ref('int_motive_safety_scores_calculation') }} 
)

, monthly_miles_driven AS ( 
    SELECT
        region AS "Region"
        , translated_site AS "Location"
        , 'Miles Driven' AS "Metric"
        {%- for month in var('months_list') %}
        , ROUND( {{month}}_miles_driven ::NUMERIC, 2) AS "{{month}}"
        {%- endfor %}
        , RANK() OVER (ORDER BY {{month_str}}_miles_driven DESC) AS "Company Rank"
        , RANK() OVER (PARTITION BY region ORDER BY {{month_str}}_miles_driven DESC) AS "Region Rank"
        , RANK() OVER (ORDER BY {{prev_month_str}}_miles_driven DESC) AS "Prev Company Rank"
        , RANK() OVER (PARTITION BY region ORDER BY {{prev_month_str}}_miles_driven DESC) AS "Prev Region Rank"
    FROM combined_dd_and_eb
)

, rounded_safety_scores AS (
    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM safety_scores_metrics
    WHERE "Location" IS NOT NULL
)
, safety_scores_ranked AS (

    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" DESC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" DESC) AS "Region Rank",
        RANK() OVER (ORDER BY "{{prev_month_str}}" DESC) AS "Prev Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" DESC) AS "Prev Region Rank"
    FROM rounded_safety_scores
    
)

, rounded_events_per_vehicle AS (
    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM final_events_per_vehicle_metrics
    WHERE "Location" IS NOT NULL
)

, final_events_per_vehicle_rank AS (

    SELECT *
        , RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank" 
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Company Rank"
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_per_vehicle

)

, rounded_events_moved_to_uncoachable AS ( 

    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM final_events_moved_to_uncoachable_metrics
    WHERE "Location" IS NOT NULL
)

, final_events_moved_to_uncoachable_rank AS ( 

    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank", 
        RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_moved_to_uncoachable
) 

, rounded_events_pending_review AS ( 

    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM final_events_pending_review_metrics 
    WHERE "Location" IS NOT NULL
)

, final_events_pending_review_rank AS ( 

    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank"
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
        , RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Company Rank"
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_events_pending_review

)

, rounded_pct_unassigned AS ( 

    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    FROM pct_unassigned_final_metrics
    WHERE "Location" IS NOT NULL
)

, pct_unassigned_final_rank AS ( 

    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank", 
        RANK() OVER (ORDER BY "{{prev_month_str}}" ASC) AS "Prev Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{prev_month_str}}" ASC) AS "Prev Region Rank"
    FROM rounded_pct_unassigned

)

, rounded_dvir AS ( 

    SELECT 
        "Region"
        , "Location"
        , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 3) AS "{{month}}"
    {%- endfor %}
    FROM dvir_metrics
    WHERE "Location" IS NOT NULL
)

, dvir_rank AS ( 

    SELECT *,
        RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ month_str }}" DESC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Metric", "Region" ORDER BY "{{ month_str }}" DESC) AS "Region Rank", 
        RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ prev_month_str }}" DESC) AS "Prev Company Rank",
        RANK() OVER (PARTITION BY "Metric", "Region" ORDER BY "{{ prev_month_str }}" DESC) AS "Prev Region Rank"
    FROM rounded_dvir
    ORDER BY "Metric", "Region", "Location"

)



, unioned_table AS (SELECT * 
FROM final_events_per_vehicle_rank
UNION ALL
SELECT * 
FROM final_events_moved_to_uncoachable_rank
UNION ALL
SELECT * 
FROM final_events_pending_review_rank
UNION ALL
SELECT * 
FROM pct_unassigned_final_rank
UNION ALL
SELECT * 
FROM safety_scores_ranked
UNION ALL 
SELECT * FROM dvir_rank
UNION ALL 
SELECT * FROM monthly_miles_driven
)

SELECT 
    *
FROM unioned_table