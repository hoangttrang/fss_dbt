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
    em.organization_level_4_id
FROM dependent_deduction d
JOIN employee e ON d.employee_id = e.id
JOIN employment em ON em.employee_id = e.id
JOIN job j ON em.primary_job_id = j.id
JOIN employee_status es ON d.employee_id = es.employee_id
ORDER BY age