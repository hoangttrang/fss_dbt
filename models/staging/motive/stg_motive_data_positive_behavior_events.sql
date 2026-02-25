WITH data_positive_behavior_events AS (
    SELECT * FROM {{ source('citus_motive', 'data_positive_behavior_events') }}
)

SELECT 
    CAST(id AS integer) AS id,
    CAST(event_id AS bigint) AS event_id,
    CAST(type AS character varying) AS type,
    CAST(driver_id AS integer) AS driver_id,
    CAST(driver_first_name AS character varying) AS driver_first_name,
    CAST(driver_last_name AS character varying) AS driver_last_name,
    CAST(vehicle_id AS integer) AS vehicle_id,
    CAST(coaching_status AS character varying) AS coaching_status,
    CAST(start_date AS timestamp with time zone) AS start_date,
    CAST(date AS date) AS date,
    CAST(secondary_behaviors AS character varying) AS secondary_behaviors,
    CAST(primary_behavior AS character varying) AS primary_behavior,
    CAST(severity AS character varying) AS severity,
    CAST(positive_behavior AS character varying) AS positive_behavior,
    CAST(group_id AS integer) AS group_id,
    CAST(group_name AS character varying) AS group_name,
    CAST(max_over_speed_in_kph AS double precision) AS max_over_speed_in_kph,
    CAST(max_over_speed_in_mph AS double precision) AS max_over_speed_in_mph,
    CAST(src AS character varying) AS src,
    CAST(created_at AS timestamp without time zone) AS created_at,
    CAST(updated_at AS timestamp without time zone) AS updated_at,
    CAST(not_current AS boolean) AS not_current
FROM data_positive_behavior_events