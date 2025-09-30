-- combine duplicate records
WITH agent_login_time_tracking AS (
    SELECT * FROM  {{ ref('stg_ringcx_agent_login_time_tracking')}}
) 


SELECT 
    -- agent info
    agent_full_name,
    agent_id,
    agent_login_id,

    -- date
    MIN(date) as agent_login_start_date, 
    CASE WHEN COUNT(DISTINCT agent_login_start_time) = 1 THEN MAX(agent_login_start_time) ELSE NULL END AS agent_login_start_time, 
    MAX(date) as agent_login_end_date, 
    CASE WHEN COUNT(DISTINCT agent_login_end_time) = 1 THEN MAX(agent_login_end_time) ELSE NULL END AS agent_login_end_time,

    -- login info
    CASE WHEN COUNT(DISTINCT total_login_time) = 1 THEN MAX(total_login_time) ELSE NULL END AS total_login_time,
    CASE WHEN COUNT(DISTINCT total_logout_time) = 1 THEN MAX(total_logout_time) ELSE NULL END AS total_logout_time,
    SUM(available) AS available,
    SUM(away) AS away,
    SUM(breaks) AS breaks,
    SUM(engaged) AS engaged,
    SUM(lunch) AS lunch,
    SUM(monitoring) AS monitoring,
    SUM(offline) AS offline,
    SUM(rna) AS rna,
    SUM(suspect) AS suspect,
    SUM(training) AS training,
    SUM(transition) AS transition,
    SUM(working) AS working
    
FROM agent_login_time_tracking
WHERE total_login_time <> 0
GROUP BY 
    agent_full_name, 
    agent_id, 
    agent_login_id
