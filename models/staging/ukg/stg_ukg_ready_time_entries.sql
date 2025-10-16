WITH time_entries AS (
    SELECT * FROM {{ source('ukg_ready', 'time_entries') }}
)

SELECT
    CAST(entry_id AS bigint) AS entry_id,
    CAST(account_id AS integer) AS account_id,
    CAST(start_date AS date) AS start_date,
    CAST(end_date AS date) AS end_date,
    CAST(date AS date) AS date,
    CAST(start_time AS timestamp with time zone) AS start_time,
    CAST(end_time AS timestamp with time zone) AS end_time,
    CAST(total AS double precision) AS total,
    CAST(approval_status AS character varying) AS approval_status,
    CAST(is_raw AS boolean) AS is_raw,
    CAST(is_calc AS boolean) AS is_calc,
    CAST(calc_start_time AS timestamp with time zone) AS calc_start_time,
    CAST(calc_end_time AS timestamp with time zone) AS calc_end_time,
    CAST(calc_total AS bigint) AS calc_total,
    CAST(piecework AS character varying) AS piecework,
    CAST(amount AS character varying) AS amount,
    CAST(custom_decimal AS character varying) AS custom_decimal,
    CAST(time_off_id AS double precision) AS time_off_id,
    CAST(cost_center_ids AS character varying) AS cost_center_ids

FROM time_entries
