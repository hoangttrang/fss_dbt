WITH sage_budget_item AS ( 
    SELECT * FROM {{ ref('int_sage_monthly_budget') }}
)

, date_spine AS (
    SELECT
        date_day::DATE AS date_day
    FROM generate_series(
        '2024-12-01'::DATE,               
        current_date + interval '1 year', 
        interval '1 day'
) AS t(date_day)

) 

,  daily_date AS (SELECT 
    date_day
    , (
          (make_date(EXTRACT(YEAR FROM date_day)::INT,
                     EXTRACT(MONTH FROM date_day)::INT, 1) + INTERVAL '1 month')::date
          - make_date(EXTRACT(YEAR FROM date_day)::INT,
                      EXTRACT(MONTH FROM date_day)::INT, 1)
        )::INT AS month_days
    , EXTRACT(YEAR  FROM date_day)::INT  AS year
    , EXTRACT(MONTH FROM date_day)::INT  AS month
FROM date_spine)

, list_of_acct_department_date AS (
    SELECT
        *
    FROM (SELECT DISTINCT acct_no, dept_no, class_id, acct_title,  dep_title, class_name
        FROM sage_budget_item)
    CROSS JOIN (SELECT * FROM daily_date)
)

, add_daily_budget AS (
    SELECT 
        list.*, 
        budget.total_amnt AS monthly_budget,
        budget.total_amnt/ month_days AS daily_budget
    FROM list_of_acct_department_date AS list
    LEFT JOIN sage_budget_item AS budget
        ON budget.acct_no = list.acct_no
        AND budget.dept_no = list.dept_no
        AND budget.class_id = list.class_id
        AND budget.report_month = list.month
        AND budget.report_year = list.year
)


SELECT 
    acct_no
    , acct_title
    , dept_no
    , dep_title
    , class_id
    , class_name
    , date_day
    , month_days 
    , monthly_budget
    , daily_budget
FROM add_daily_budget
WHERE monthly_budget IS NOT NULL