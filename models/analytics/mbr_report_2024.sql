
{% set month_str = get_current_month_str() %}

WITH monthly_events AS (
    SELECT * FROM {{ ref('int_motive_all_monthly_events') }}
)

, vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
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

, dvir_rank AS ( 
    SELECT * FROM {{ ref('int_motive_dvir_rank') }}
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

, uncoachable_base_grid AS (
    SELECT
		DISTINCT
        vm.region AS "Region",
        vm.translated_site AS "Location",
        'Events Moved to Uncoachable' AS "Metric"
    FROM vehicle_map_rs vm
	WHERE translated_site is NOT NULL
)

, final_events_moved_to_uncoachable AS (
    SELECT 
        bg."Region",
        bg."Location",
        bg."Metric"
        {% for month in var('months_list') %}
        , COALESCE(dc."{{ month  }}", 0) AS "{{ month  }}"
         {% endfor %}
    FROM uncoachable_base_grid bg
    LEFT JOIN ranked_events_moved_to_uncoachable dc
        ON bg."Region" = dc."Region"
        AND bg."Location" = dc."Location"
        AND bg."Metric" = dc."Metric"
)

, final_events_moved_to_uncoachable_rank AS 
(
    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM final_events_moved_to_uncoachable
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

, final_events_pending_review_rank AS (
    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM final_events_pending_review
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

, pct_unassigned_final_rank AS (
    SELECT *,
        RANK() OVER (ORDER BY "{{month_str}}" ASC) AS "Company Rank",
        RANK() OVER (PARTITION BY "Region" ORDER BY "{{month_str}}" ASC) AS "Region Rank"
    FROM pct_unassigned_final
    where "Location" is not null
)



SELECT * 
FROM final_events_per_vehicle
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

