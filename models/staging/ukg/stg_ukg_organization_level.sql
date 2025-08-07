WITH organization_level AS (
    SELECT * FROM {{ source ('ukg', 'organization_level') }}
)

SELECT
    CAST(id AS character varying) AS id,
    CAST(level AS integer) AS level,
    CAST(_fivetran_deleted AS boolean) AS _fivetran_deleted,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(is_active AS boolean) AS is_active,
    CAST(level_description AS character varying) AS level_description,
    CAST(current_year_budget_salary AS double precision) AS current_year_budget_salary,
    CAST(gl_segment AS character varying) AS gl_segment,
    CAST(last_year_budget_fte AS double precision) AS last_year_budget_fte,
    CAST(description AS character varying) AS description,
    CAST(current_year_budget_fte AS double precision) AS current_year_budget_fte,
    CAST(last_year_budget_salary AS double precision) AS last_year_budget_salary
FROM organization_level