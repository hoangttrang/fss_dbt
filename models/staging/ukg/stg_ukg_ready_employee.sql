WITH employee AS (
    SELECT * FROM {{ source('ukg_ready', 'employee') }}
)

SELECT
    CAST(id AS bigint) AS id,
    CAST(employee_id AS character varying) AS employee_id,
    CAST(username AS character varying) AS username,
    CAST(first_name AS character varying) AS first_name,
    CAST(last_name AS character varying) AS last_name,
    CAST(status AS character varying) AS status,
    CAST(hired_date AS date) AS hired_date,
    CAST(started_date AS date) AS started_date,
    CAST(terminated_date AS date) AS terminated_date,
    CAST(re_hired_date AS date) AS re_hired_date

FROM employee
