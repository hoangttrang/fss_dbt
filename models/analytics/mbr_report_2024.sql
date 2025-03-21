

WITH monthly_events AS (
    SELECT * FROM {{ ref('int_motive_all_monthly_events') }}
)

, safety_scores_ranked AS (
    SELECT * FROM {{ ref('int_motive_safety_scores_ranked') }}
)

, final_events_per_vehicle AS (
    SELECT * FROM {{ ref('int_motive_events_p_vehicles_ranked') }}
)

, rank_trips AS ( 
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, ranked_events_moved_to_uncoachable AS (
    SELECT 
        region as "Region"
        , translated_site as "Location"
        , 'Events Moved to Uncoachable' as "Metric"
        {%- for month in var('months_list') -%}
        , COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN event_id END) AS "{{ month }}"
        {%- endfor %}
    FROM monthly_events 
    WHERE 1=1 AND coaching_status = 'uncoachable'
    GROUP BY region, translated_site 
)

, final_events_moved_to_uncoachable AS 
(
    SELECT *,
        RANK() OVER (ORDER BY "December" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "December" ASC) AS "Region Rank"
    FROM ranked_events_moved_to_uncoachable
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
    SELECT *,
        RANK() OVER (ORDER BY "December" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "December" ASC) AS "Region Rank"
    FROM ranked_events_pending_review
)

, pct_unassigned_ranked AS (
    SELECT
        region AS "Region"
        , translated_site AS "Location"
        , 'Unidentified Trips' AS "Metric"
        {%- for month in var('months_list') -%}
        , COALESCE(CAST(COUNT(DISTINCT CASE WHEN unassigned = true AND month = '{{ month }}' THEN event_id END) AS FLOAT) 
        / NULLIF(COUNT(DISTINCT CASE WHEN month = '{{ month }}' THEN event_id END), 0), 0) AS "{{ month }}"
        {%- endfor %}
    FROM rank_trips
    WHERE 1=1
    AND row_num = 1
    GROUP BY region, translated_site, "Metric"
)

, pct_unassigned_final AS (
    SELECT *,
        RANK() OVER (ORDER BY "December" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "December" ASC) AS "Region Rank"
    FROM pct_unassigned_ranked
    where "Location" is not null
)

SELECT * 
FROM final_events_per_vehicle
UNION ALL
SELECT * 
FROM final_events_moved_to_uncoachable
UNION ALL
SELECT * 
FROM final_events_pending_review
UNION ALL
SELECT * 
FROM pct_unassigned_final
UNION ALL
SELECT * 
FROM safety_scores_ranked
 

