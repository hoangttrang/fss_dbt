WITH safety_score_weights AS (
    SELECT * FROM {{ source ('citus_motive', 'safety_score_weights') }}
)

SELECT 
    *
FROM safety_score_weights