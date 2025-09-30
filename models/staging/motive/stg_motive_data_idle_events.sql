WITH data_idle_events AS (
    SELECT * FROM {{ source ('citus_motive', 'data_idle_events')}}
)

SELECT 
    CAST(id AS integer) AS id,
    CAST(event_id AS bigint) AS event_id,
    CAST(start_time AS timestamp with time zone) AS start_time,
    CAST(end_time AS timestamp with time zone) AS end_time,
    CAST(vehicle_id AS integer) AS vehicle_id,
    CAST(driver_id AS integer) AS driver_id,
    CAST(driver_company_id AS character varying) AS driver_company_id,
    CAST(minutes_idling AS double precision) AS minutes_idling
FROM data_idle_events

