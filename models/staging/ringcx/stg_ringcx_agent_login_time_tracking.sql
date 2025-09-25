WITH agent_login_time_tracking AS (
    SELECT * FROM  {{ source ('ringcx', 'agent_login_time_tracking')}}
) 


SELECT
    CAST(date AS date) AS date,
    CAST(agent_full_name AS character varying) AS agent_full_name,
    CAST(agent_id AS integer) AS agent_id,
    CAST(agent_login_id AS integer) AS agent_login_id,
    CAST(agent_login_start_time AS character varying) AS agent_login_start_time,
    CAST(agent_login_end_time AS character varying) AS agent_login_end_time,
    CAST(agent_first_login_time AS character varying) AS agent_first_login_time,
    CAST(agent_last_logout_time AS character varying) AS agent_last_logout_time,
    CAST(total_login_time AS double precision) AS total_login_time,
    CAST(total_logout_time AS double precision) AS total_logout_time,
    CAST(available AS double precision) AS available,
    CAST(away AS double precision) AS away,
    CAST(breaks AS double precision) AS breaks,
    CAST(engaged AS double precision) AS engaged,
    CAST(lunch AS double precision) AS lunch,
    CAST(monitoring AS double precision) AS monitoring,
    CAST(offline AS double precision) AS offline,
    CAST(rna AS double precision) AS rna,
    CAST(suspect AS double precision) AS suspect,
    CAST(training AS double precision) AS training,
    CAST(transition AS double precision) AS transition,
    CAST(working AS double precision) AS working,
    CAST(updated_at AS timestamp without time zone) AS updated_at,
    CAST(id AS bigint) AS id
FROM agent_login_time_tracking