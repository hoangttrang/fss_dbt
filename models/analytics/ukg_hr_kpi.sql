WITH dependent_deduction AS (
    SELECT * FROM {{ ref('stg_ukg_dependent_deduction') }}
)

, employee AS (
    SELECT * FROM {{ ref('stg_ukg_employee') }}

), employment AS (
    SELECT * FROM {{ ref('stg_ukg_employment') }}

), job AS (
    SELECT * FROM {{ ref('stg_ukg_job') }}
    
), employee_status AS (
    SELECT * FROM {{ ref('stg_ukg_employee_status') }}
)

, distinct_dependent_deduction AS (
    SELECT DISTINCT  employee_id, type
    FROM dependent_deduction

)
, gl_translation AS (
    SELECT * FROM {{ ref('int_ukg_gl_location') }}
)

SELECT 
    d.type,
    e.id AS employee_id,
    em.date_of_seniority,
    em.job_description,
    j.job_family_code,
    DATE_PART('year', AGE(CURRENT_DATE, e.date_of_birth)) AS age,
    e.military_service,
    e.is_disabled,
    e.ethnic_description,
    es.status,
    em.organization_level_4_id AS site_id,
    gl_translation.description AS site_description,
    em.date_of_termination
FROM employee e
LEFT JOIN distinct_dependent_deduction d 
    ON d.employee_id = e.id
LEFT JOIN employment em
    ON em.employee_id = e.id
LEFT JOIN job j 
    ON em.primary_job_id = j.id
LEFT JOIN employee_status es 
    ON e.id = es.employee_id
LEFT JOIN gl_translation 
    ON em.organization_level_4_id = gl_translation.code