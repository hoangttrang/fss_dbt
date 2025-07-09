
{% set month_str = get_current_month_str() %}

WITH safety_scores_metrics AS (
    SELECT * FROM {{ ref('int_motive_safety_scores_metrics') }}
)

, final_events_per_vehicle_metrics AS (
    SELECT * FROM {{ ref('int_motive_events_p_vehicles_metrics') }}
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


, safety_scores_ranked AS (

    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" DESC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" DESC) AS "Region Rank"
    FROM safety_scores_metrics
    WHERE "Location" IS NOT NULL
    
)

, final_events_per_vehicle_rank AS (

    SELECT *
        , RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank" 
        , RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM final_events_per_vehicle_metrics
    WHERE "Location" IS NOT NULL

)

, final_events_moved_to_uncoachable_rank AS ( 

    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM final_events_moved_to_uncoachable_metrics
    WHERE "Location" IS NOT NULL

) 

, final_events_pending_review_rank AS ( 

    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM final_events_pending_review_metrics 
    WHERE "Location" IS NOT NULL

)

, pct_unassigned_final_rank AS ( 

    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM pct_unassigned_final_metrics
    WHERE "Location" IS NOT NULL

)

, dvir_rank AS ( 

    SELECT *,
        RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ month_str }}" DESC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Metric", "Region" ORDER BY "{{ month_str }}" DESC) AS "Region Rank"
    FROM dvir_metrics
    WHERE "Location" IS NOT NULL
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
) 

SELECT 
    "Region"
    , "Location"
    , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 2) AS "{{month}}"
    {%- endfor %}
    , "Company Rank"
    , "Region Rank"
FROM unioned_table