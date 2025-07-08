WITH dependent_deduction AS (
    SELECT * FROM {{ ref('stg_ukg_dependent_deduction') }}
)

, employee AS (
    SELECT * FROM {{ ref('stg_ukg_employee') }}
)

, employment AS (
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, job AS (
    SELECT * FROM {{ ref('stg_ukg_job') }}    
)

, compensation  AS (
    SELECT * FROM {{ ref('stg_ukg_compensation') }}
)

, active_employee AS (
    SELECT * FROM {{ ref ('int_ukg_active_employee') }}
)

, distinct_dependent_deduction AS (
    SELECT
        employee_id
        , MAX(CASE WHEN type = 'DEN' THEN 1 ELSE 0 END) AS is_dental
        , MAX(CASE WHEN type = 'MED' THEN 1 ELSE 0 END) AS is_medical
        , MAX(CASE WHEN type = 'VIS' THEN 1 ELSE 0 END) AS is_vision
    FROM dependent_deduction
    WHERE type IN ('DEN', 'MED', 'VIS')
    GROUP BY employee_id
)

, gl_translation AS (
    SELECT * FROM {{ ref('int_ukg_gl_location') }}
)

SELECT 
    e.id AS employee_id,
    em.date_of_seniority,
    em.original_hire_date,
    em.job_description,
    j.job_family_code
    , COALESCE(d.is_dental, 0) AS is_dental
    , COALESCE(d.is_medical, 0) AS is_medical
    , COALESCE(d.is_vision, 0) AS is_vision
    , em.employee_type_code,
    em.full_time_or_part_time_code,
    DATE_PART('year', AGE(CURRENT_DATE, e.date_of_birth)) AS age,
    e.military_service,
    e.is_disabled,
    e.gender, 
    e.ethnic_description,
    compensation.annual_salary, 
    em.organization_level_4_id AS site_id,
    gl_translation.description AS site_description,
    em.date_of_termination,
    CASE WHEN ae.id IS NOT NULL THEN 1 ELSE 0 END AS is_active,
    ae.potential_active_reason
FROM employee e
LEFT JOIN distinct_dependent_deduction d 
    ON d.employee_id = e.id
LEFT JOIN employment em
    ON em.employee_id = e.id
LEFT JOIN job j 
    ON em.primary_job_id = j.id
LEFT JOIN gl_translation 
    ON em.organization_level_4_id = gl_translation.code
LEFT JOIN compensation
    ON e.id = compensation.employee_id
    AND em.primary_job_id = compensation.primary_job_id
LEFT JOIN active_employee ae -- join on employee_id/id 
    ON e.id = ae.id
