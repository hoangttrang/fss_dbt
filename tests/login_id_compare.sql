WITH raw_table_count AS(
    SELECT COUNT(DISTINCT agent_login_id) AS raw_count
    FROM  {{ source ('ringcx', 'agent_login_time_tracking')}}
    WHERE total_login_time <> 0
    

),agg_table_count AS(
    SELECT COUNT (agent_login_id) AS agg_count
    FROM {{ref('ringcx_agent_login_time_agg')}}
)

SELECT raw_count
FROM raw_table_count
    CROSS JOIN agg_table_count 
WHERE raw_table_count.raw_count <> agg_table_count.agg_count