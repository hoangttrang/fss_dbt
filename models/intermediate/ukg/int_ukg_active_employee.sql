-- This file is used to create a view that identifies potential active employees based on their employment status and recent activity.


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

, active_employee AS (
    SELECT 
        employee.*
    FROM employee
    LEFT JOIN employee_status status
        ON employee.id = status.employee_id
    WHERE 
        status.status IN ('A', 'L') 
        AND employee.id IS NOT NULL
) 


, active_employment AS (
    SELECT * FROM employment
    WHERE date_of_termination IS NULL
)

-- This CTE defines the active employees based on if they are recently hired or have they had any activity.
, earnings_employment AS (
    SELECT 
        employment.employee_id
        , employment.id AS employment_id
        , CASE 
            WHEN earning.employment_id IS NOT NULL THEN 'Has Earnings'
            WHEN EXTRACT(YEAR FROM  employment.date_of_seniority) = 2025 
				AND EXTRACT(MONTH FROM employment.date_of_seniority) = EXTRACT(MONTH FROM CURRENT_DATE)
				THEN 'Hired in ' || TO_CHAR(employment.date_of_seniority, 'MM-YYYY')
            ELSE 'No Activity'
          END AS potential_active_reason
        , CASE 
            WHEN earning.employment_id IS NOT NULL 
                 OR (EXTRACT(YEAR FROM employment.date_of_seniority) = 2025 
				 	AND EXTRACT(MONTH FROM employment.date_of_seniority) = EXTRACT(MONTH FROM CURRENT_DATE))
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

, active_employees AS (
    SELECT 
        active_employee.*
        , earnings_employment.potential_active_reason
        , earnings_employment.is_potential_active
    FROM active_employee
    INNER JOIN active_employment
        ON active_employee.id = active_employment.employee_id
    INNER JOIN earnings_employment 
        ON active_employee.id = earnings_employment.employee_id
    WHERE 
      -- filter to only include employees who are potential active
        earnings_employment.is_potential_active = 1
)

SELECT 
    * 
FROM active_employees