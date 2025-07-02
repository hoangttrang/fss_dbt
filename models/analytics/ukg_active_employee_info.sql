WITH employee AS (
    SELECT * FROM {{ ref('stg_ukg_employee') }}
)

, employment AS (
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, potential_active_employee AS (
    SELECT * FROM {{ ref('int_ukg_potential_active_employee') }}
)

, gl_translation AS (
    SELECT * FROM {{ ref('int_ukg_gl_location') }}
)

, dependent_deduction AS (
    SELECT * FROM {{ ref('stg_ukg_dependent_deduction') }}
)

, employee_status AS (
    SELECT * FROM {{ ref('stg_ukg_employee_status') }}
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

, job AS (
    SELECT * FROM {{ ref('stg_ukg_job') }}  
)

, active_employees AS (
    SELECT 
        DISTINCT 
        employee.id AS employee_id
        , employment.id AS employment_id
        , employment.date_of_seniority
        , employment.original_hire_date
        , deduction.is_dental
        , deduction.is_medical
        , deduction.is_vision   
        , employment.employee_type_code
        , employment.full_time_or_part_time_code
        , employment.job_description
        , job.job_family_code
        , employee.date_of_birth
        , DATE_PART('year', AGE(CURRENT_DATE, employee.date_of_birth)) AS age
        , employee.military_service
        , employee.is_disabled
        , employee.ethnic_description
        , employee.gender
        , employment.organization_level_4_id AS site_id
        , gl_translation.description AS site_description
    FROM employee
    LEFT JOIN employee_status status
        ON employee.id = status.employee_id
    LEFT JOIN employment
        ON employee.id = employment.employee_id
    LEFT JOIN distinct_dependent_deduction deduction
        ON deduction.employee_id = employee.id
    LEFT JOIN job 
        ON employment.primary_job_id = job.id
    LEFT JOIN gl_translation 
        ON employment.organization_level_4_id = gl_translation.code
    WHERE 
        status.status IN ('A', 'L') 
        AND employment.id IS NOT NULL
        AND employment.date_of_termination IS NULL
        -- filter to only include employees who are potential active
        AND employee.id IN (
            SELECT DISTINCT id
            FROM potential_active_employee
            WHERE is_potential_active = 1
    )
) 

SELECT 
    employee_id
    , employment_id
    , date_of_seniority
    , original_hire_date
    , COALESCE(is_dental, 0) AS is_dental
    , COALESCE(is_medical, 0) AS is_medical
    , COALESCE(is_vision, 0) AS is_vision
    , job_description
    , job_family_code
    , date_of_birth
    , age
    , military_service
    , is_disabled
    , ethnic_description
    , gender
    , site_id
    , site_description
FROM active_employees