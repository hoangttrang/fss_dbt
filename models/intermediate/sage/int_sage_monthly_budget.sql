WITH gl_budget_item AS (
    SELECT * FROM {{ ref('stg_sage_gl_budget_item') }}
)

SELECT 
    acct_no 
    , EXTRACT(YEAR FROM p_start_date) AS report_year
    , EXTRACT(MONTH FROM p_start_date) AS report_month
    , dept_no
    , class_id 
    , dep_title
    , class_name 
    , period_key
    , reporting_period_name 
    , SUM(amount) AS total_amnt
FROM gl_budget_item
WHERE acct_no IN (40001, 40010, 40097, 40099, 40100)
GROUP BY 
    acct_no 
    , EXTRACT(YEAR FROM p_start_date)
    , EXTRACT(MONTH FROM p_start_date)
    , dept_no
    , class_id 
    , dep_title
    , class_name 
    , period_key
    , reporting_period_name 
ORDER BY 
    acct_no, 
    period_key, 
    dept_no, 
    class_id