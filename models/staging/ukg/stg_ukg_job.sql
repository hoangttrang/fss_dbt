WITH job AS (
    SELECT * FROM {{ source('ukg', 'job') }}
)

SELECT
    CAST(id AS VARCHAR) AS id,
    CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted,
    CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced,
    CAST(country_code AS VARCHAR) AS country_code,
    CAST(is_active AS BOOLEAN) AS is_active,
    CAST(job_family_code AS VARCHAR) AS job_family_code,
    CAST(long_description AS VARCHAR) AS long_description,
    CAST(title AS VARCHAR) AS title,
    CAST(eeo_category AS VARCHAR) AS eeo_category,
    CAST(flsa_type_code AS VARCHAR) AS flsa_type_code
FROM job


