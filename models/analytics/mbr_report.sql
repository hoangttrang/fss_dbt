

WITH  safety_scores_ranked AS (
    SELECT * FROM {{ ref('int_motive_safety_scores_ranked') }}
)

, final_events_per_vehicle AS (
    SELECT * FROM {{ ref('int_motive_events_p_vehicles_ranked') }}
)

, final_events_moved_to_uncoachable_rank AS ( 
    SELECT * FROM {{ ref('int_motive_event_uncoachable_rank') }}
)

, final_events_pending_review_rank AS ( 
    SELECT * FROM {{ ref('int_motive_event_pending_review_rank') }}
)

, pct_unassigned_final_rank AS ( 
    SELECT * FROM {{ ref('int_motive_event_unassigned_rank') }}
)

, dvir_rank AS ( 
    SELECT * FROM {{ ref('int_motive_dvir_rank') }}
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

