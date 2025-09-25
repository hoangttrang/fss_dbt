WITH agent_login_time_tracking AS (
    SELECT * FROM  {{ source ('ringcx', 'hourly_inbound_interaction_stats')}}
) 

SELECT 
    CAST(queue_group AS character varying) AS queue_group,
    CAST(queue_group_id AS integer) AS queue_group_id,
    CAST(queue AS character varying) AS queue,
    CAST(queue_id AS integer) AS queue_id,
    CAST(started_hour AS integer) AS started_hour,
    CAST(date_interaction_start AS date) AS date_interaction_start,
    CAST(presented AS integer) AS presented,
    CAST(deflected AS integer) AS deflected,
    CAST(accepted AS integer) AS accepted,
    CAST(abandoned AS integer) AS abandoned,
    CAST(short_abandoned AS integer) AS short_abandoned,
    CAST(manual_dial AS integer) AS manual_dial,
    CAST(manual_dial_no_connect AS integer) AS manual_dial_no_connect,
    CAST(successful AS integer) AS successful,
    CAST(queue_time AS double precision) AS queue_time,
    CAST(handle_time AS double precision) AS handle_time,
    CAST(avg_queue_time AS double precision) AS avg_queue_time,
    CAST(avg_queue_abandon_time AS double precision) AS avg_queue_abandon_time,
    CAST(avg_speed_of_answer AS double precision) AS avg_speed_of_answer,
    CAST(avg_talk_time AS double precision) AS avg_talk_time,
    CAST(avg_wrap_time AS double precision) AS avg_wrap_time,
    CAST(avg_agent_handle_time AS double precision) AS avg_agent_handle_time,
    CAST(sla_passed AS integer) AS sla_passed,
    CAST(queue_interaction_service_level AS double precision) AS queue_interaction_service_level,
    CAST(updated_at AS timestamp without time zone) AS updated_at,
    CAST(id AS bigint) AS id
FROM agent_login_time_tracking