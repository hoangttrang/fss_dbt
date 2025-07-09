WITH employment AS (
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)


, earning_history_base_element AS (
    SELECT * FROM {{ ref('stg_ukg_earning_history_base_element') }}
)

, gl_translation AS (
    SELECT 
    * FROM {{ ref('int_ukg_gl_location') }}
)
, pay_period AS (select 
	eh.employee_id 
, eh.employment_id 
, eh.job_id
, eh.pay_date
, eh.tax_category
, eh.current_amount
, em.organization_level_4_id
, gl.description AS site_description
FROM earning_history_base_element eh
LEFT JOIN employment em
   ON em.id = eh.employment_id
   AND em.employee_id = eh.employee_id
LEFT JOIN gl_translation gl
   ON em.organization_level_4_id = gl.code
WHERE eh._fivetran_deleted = 'false'
)


SELECT 
    pay_date
    , site_description
    , SUM(current_amount) AS total_weekly_salary
FROM pay_period
GROUP BY 
    pay_date
    , site_description
    