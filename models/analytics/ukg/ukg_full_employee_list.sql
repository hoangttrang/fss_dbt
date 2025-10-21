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

, pay_register AS (
    SELECT * FROM {{ ref('stg_ukg_pay_register') }}
)

, active_employee AS (
    SELECT * FROM {{ ref ('int_ukg_active_employee') }}
)

, latest_annual_salary AS (
    SELECT 
        t1.employee_id,
        t1.employment_id,
        MAX(t1.annual_salary) AS latest_annual_salary,
        MAX(t1.hourly_pay_rate) AS latest_hourly_pay_rate,
        t1.pay_date AS latest_pay_date
    FROM pay_register t1
    INNER JOIN (
        SELECT 
            employee_id,
            employment_id,
            MAX(pay_date) as pay_date
        FROM pay_register
        GROUP BY employee_id
            , employment_id
    ) t2 ON t1.employee_id = t2.employee_id 
        AND t1.pay_date = t2.pay_date
        AND t1.employment_id = t2.employment_id
    GROUP BY 
        t1.employee_id,
        t1.employment_id,
        t1.pay_date
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

, final_table AS (SELECT 
    e.id AS employee_id,
    e.first_name,
    e.last_name,
    e.preferred_name, 
    e.email_address,
    em.id AS employment_id,
    em.date_of_seniority,
    em.last_hire_date, -- last hire date is when the employee got rehired
    em.original_hire_date,
    em.hire_source, 
    em.job_description,
    em.term_type,
    em.term_reason,
    em.termination_reason_description,
    j.id AS job_id,
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
    latest_annual_salary.latest_annual_salary, 
    latest_annual_salary.latest_hourly_pay_rate,
    latest_annual_salary.latest_pay_date,
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
LEFT JOIN latest_annual_salary
    ON e.id = latest_annual_salary.employee_id
    AND em.id = latest_annual_salary.employment_id
LEFT JOIN active_employee ae -- join on employee_id/id 
    ON e.id = ae.id
) 

SELECT 
    DISTINCT * 
FROM final_table
WHERE first_name != 'Test' and last_name != 'Employee'