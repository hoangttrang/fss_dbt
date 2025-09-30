WITH agent_voice_state AS (
    SELECT * FROM  {{ source ('ringcx', 'agent_voice_state')}}
) 

SELECT
    CAST(agent_group AS character varying) AS agent_group,
    CAST(agent_group_id AS integer) AS agent_group_id,
    CAST(agent_full_name AS character varying) AS agent_full_name,
    CAST(agent_id AS integer) AS agent_id,
    CAST(base_state AS character varying) AS base_state,
    CAST(date AS date) AS date,
    CAST(no_pending_disposition_time AS integer) AS no_pending_disposition_time,
    CAST(pending_disposition_time AS integer) AS pending_disposition_time,
    CAST(agent_voice_state_time AS integer) AS agent_voice_state_time,
    CAST(updated_at AS timestamp without time zone) AS updated_at,
    CAST(id AS bigint) AS id
FROM agent_voice_state