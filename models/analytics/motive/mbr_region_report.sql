{% set month_str = get_current_month_str() %}

WITH dvir_region_metrics_region AS (
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

-- Give Region Ranking for metrics 
, dvir_rank_region AS ( 
    SELECT *,
        RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ month_str }}" DESC) AS "Region Rank"
    FROM dvir_region_metrics_region
    ORDER BY "Metric", "Region"
)

, events_per_vehicle_rank_region AS (
    SELECT *
        , RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ month_str }}" DESC) AS "Region Rank"
    FROM events_per_vehicle_metrics_region
)

, events_pending_review_rank_region AS ( 
    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM event_pending_review_region_metrics 
)

, pct_unassigned_final_rank_region AS ( 
    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM pct_unassigned_final_metrics_region
)

, events_moved_to_uncoachable_rank_region AS ( 
    SELECT *, 
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM events_moved_to_uncoachable_metrics
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
)

, final_table AS (SELECT 
    "Region"
    , "Metric"
    {%- for month in var('months_list') %}
        , ROUND("{{month}}"::NUMERIC, 3) AS "{{month}}"
    {%- endfor %}
    , "Region Rank"
FROM union_table)

select * from final_table