WITH employee AS (
    SELECT * FROM {{ ref('stg_ukg_employee') }}
)

, employment AS (
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, earning_history_base_element AS (
    SELECT * FROM {{ ref('stg_ukg_earning_history_base_element') }}
)

, employee_status AS (
    SELECT * FROM {{ ref('stg_ukg_employee_status') }}
)

-- This CTE defines the active employees based on if they are recently hired or have they had any activity.
, earnings_employment AS (
    SELECT 
        employment.employee_id
        , employment.id AS employment_id
        , employment.original_hire_date
        , CASE 
            WHEN earning.employment_id IS NOT NULL THEN 'Has Earnings'
            WHEN EXTRACT(YEAR FROM employment.original_hire_date) = 2025 THEN 'Hired in 2025'
            ELSE 'No Activity'
          END AS potential_active_reason
        , CASE 
            WHEN earning.employment_id IS NOT NULL 
                 OR EXTRACT(YEAR FROM employment.original_hire_date) = 2025 
            THEN 1 
            ELSE 0
          END AS is_potential_active
    FROM employment
    LEFT JOIN (
        SELECT DISTINCT employment_id 
        FROM earning_history_base_element 
        WHERE employment_id IS NOT NULL
    ) earning 
        ON employment.id = earning.employment_id
    ORDER BY employment.id
)

, potential_active_tab AS (
    SELECT * 
    FROM earnings_employment 
    WHERE is_potential_active = 1
)

, active_employees AS (
    SELECT 
        employee.*
    FROM employee
    LEFT JOIN employee_status status
        ON employee.id = status.employee_id
    LEFT JOIN employment
        ON employee.id = employment.employee_id
    WHERE 
        status.status IN ('A', 'L') 
        AND employment.id IS NOT NULL
        AND employment.date_of_termination IS NULL
        AND employee.id IN (
            SELECT DISTINCT employee_id
            FROM potential_active_tab
        )
)

SELECT 
    * 
FROM active_employees
