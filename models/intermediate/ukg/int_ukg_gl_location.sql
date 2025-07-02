WITH organization_level AS (
    SELECT * FROM {{ ref('stg_ukg_organization_level') }}
)

SELECT 
    DISTINCT
        id AS code, 
        description, 
        is_active AS status
FROM organization_level
WHERE level = 4 