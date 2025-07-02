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
    SELECT * FROM {{ ref('gl_translation') }}
)
SELECT 
        employee.id
        , employee.date_of_senority
        , employee.original_hire_date
        , employee.date_of_birth
        , DATE_PART('year', AGE(CURRENT_DATE, employee.date_of_birth)) AS age
        , employee.military_service
        , employee.is_disabled
        , employee.ethnic_description
        , em.organization_level_4_id AS site_id,
        , gl_translation.description AS site_description,
    FROM employee
    LEFT JOIN employee_status status
        ON employee.id = status.employee_id
    LEFT JOIN employment
        ON employee.id = employment.employee_id
    LEFT JOIN gl_translation 
        ON employment.organization_level_4_id = gl_translation.code
    WHERE 
        status.status IN ('A', 'L') 
        AND employment.id IS NOT NULL
        AND employment.date_of_termination IS NULL
        AND employee.id IN (
            SELECT DISTINCT employee_id
            FROM potential_active_tab
        )
)