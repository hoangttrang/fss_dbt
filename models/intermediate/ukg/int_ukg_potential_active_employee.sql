-- Get current month 
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
        , employment.date_of_seniority
        , CASE 
            WHEN employment.id IS NOT NULL THEN 'Has Earnings'
            WHEN EXTRACT(YEAR FROM  employment.date_of_seniority) = 2025 
				AND EXTRACT(MONTH FROM employment.date_of_seniority) = EXTRACT(MONTH FROM CURRENT_DATE)
				THEN 'Hired in ' || TO_CHAR(employment.date_of_seniority, 'MM-YYYY')
            ELSE 'No Activity'
          END AS potential_active_reason
        , CASE 
            WHEN employment.id IS NOT NULL 
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

, potential_active_tab AS (
    SELECT * 
    FROM earnings_employment 
    WHERE is_potential_active = 1
)


SELECT 
    *
FROM potential_active_tab
