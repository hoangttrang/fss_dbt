WITH motive_ukg_mapping AS (
    SELECT * FROM {{ source ('citus_motive', 'motive_ukg_mapping')}}
)

SELECT
    CAST(id AS integer) AS id,
    CAST(ukg_id AS character varying) AS ukg_id,
    CAST(motive_id AS character varying) AS motive_id,
    CAST(employee_id AS character varying) AS employee_id,
    CAST(job_family_code AS character varying) AS job_family_code,
    CAST(first_name AS character varying) AS first_name,
    CAST(last_name AS character varying) AS last_name,
    CAST(status AS character varying) AS status,
    CAST(match_tier AS character varying) AS match_tier,
    CAST(created_at AS timestamp without time zone) AS created_at,
    CAST(updated_at AS timestamp without time zone) AS updated_at
FROM motive_ukg_mapping