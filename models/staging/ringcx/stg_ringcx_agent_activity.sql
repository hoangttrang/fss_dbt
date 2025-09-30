WITH agent_activity AS (
    SELECT * FROM  {{ source ('ringcx', 'agent_activity')}}
) 

SELECT 
    CAST(agent_group AS character varying) AS agent_group,
    CAST(agent_group_id AS integer) AS agent_group_id,
    CAST(agent_full_name AS character varying) AS agent_full_name,
    CAST(agent_id AS integer) AS agent_id,
    CAST(date AS date) AS date,
    CAST(interactions_assigned AS integer) AS interactions_assigned,
    CAST(interactions_handled AS integer) AS interactions_handled,
    CAST(in_interaction_handled AS integer) AS in_interaction_handled,
    CAST(ob_interaction_handled AS integer) AS ob_interaction_handled,
    CAST(acceptance_rate AS double precision) AS acceptance_rate,
    CAST(rna AS integer) AS rna,
    CAST(rna_percent AS double precision) AS rna_percent,
    CAST(interactions_unclassified AS integer) AS interactions_unclassified,
    CAST(manual_dial_rate AS double precision) AS manual_dial_rate,
    CAST(manual_dial_no_connect_rate AS double precision) AS manual_dial_no_connect_rate,
    CAST(avg_agent_handle_time AS double precision) AS avg_agent_handle_time,
    CAST(avg_ring_time AS double precision) AS avg_ring_time,
    CAST(avg_talk_time AS double precision) AS avg_talk_time,
    CAST(avg_hold_time AS double precision) AS avg_hold_time,
    CAST(avg_wrap_time AS double precision) AS avg_wrap_time,
    CAST(login_time AS double precision) AS login_time,
    CAST(voice_agent_login_utilization AS double precision) AS voice_agent_login_utilization,
    CAST(voice_agent_occupancy_rate AS double precision) AS voice_agent_occupancy_rate,
    CAST(digital_agent_login_utilization AS double precision) AS digital_agent_login_utilization,
    CAST(digital_agent_occupancy_rate AS double precision) AS digital_agent_occupancy_rate,
    CAST(updated_at AS timestamp without time zone) AS updated_at,
    CAST(id AS bigint) AS id
FROM agent_activity