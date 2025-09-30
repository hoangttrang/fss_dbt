/*
The way how these calculation work is that, for every week, it will tell us for that week, what is the start month, and what is the
end month. For example 
week_start_date = 2024-07-29 - Monday and week_end_date = 2024-08-04 - Sunday 
once you get the month and year for each weeks date, you will find the how many days are in that month for both the start month and end month.

We will then use that information to get budget for each week, the calculation is taking the budget for each start and end month then divide the number of days are of the month each week have 
For example from above, the budget for that week will be partially from July and partially from August
*/

WITH date_map AS ( 
    SELECT DISTINCT * FROM {{ ref('int_dims_week_mon_to_sun') }}
)

, sage_budget_item AS ( 
    SELECT * FROM {{ ref('int_sage_monthly_budget') }}
)

, list_of_acct_department_date AS (
    SELECT
        *
    FROM (SELECT DISTINCT acct_no, dept_no, class_id, dep_title, acct_title, class_name
        FROM sage_budget_item)
    CROSS JOIN (SELECT * FROM date_map)
)

, add_budget AS (
    SELECT 
        list.* 
        ,  (
            (make_date(EXTRACT(YEAR FROM list.week_start_date)::INT,
                        EXTRACT(MONTH FROM list.week_start_date)::INT, 1) + INTERVAL '1 month')::date
            - make_date(EXTRACT(YEAR FROM list.week_start_date)::INT,
                        EXTRACT(MONTH FROM list.week_start_date)::INT, 1)
            )::INT AS start_month_days
        ,  (
            (make_date(EXTRACT(YEAR FROM list.week_end_date)::INT,
                        EXTRACT(MONTH FROM list.week_end_date)::INT, 1) + INTERVAL '1 month')::date
            - make_date(EXTRACT(YEAR FROM list.week_end_date)::INT,
                        EXTRACT(MONTH FROM list.week_end_date)::INT, 1)
            )::INT AS end_month_days
        , budget_month1.total_amnt AS budget_month_start
        , budget_month2.total_amnt AS budget_month_end
    FROM list_of_acct_department_date AS list
    LEFT JOIN sage_budget_item AS budget_month1 
    ON budget_month1.acct_no = list.acct_no
        AND budget_month1.dept_no = list.dept_no
        AND budget_month1.class_id = list.class_id
        AND budget_month1.report_year = list.year_start_num
        AND budget_month1.report_month = list.month_start_num
    LEFT JOIN sage_budget_item AS budget_month2
    ON budget_month2.acct_no = list.acct_no
        AND budget_month2.dept_no = list.dept_no
        AND budget_month2.class_id = list.class_id
        AND budget_month2.report_year = list.year_end_num
        AND budget_month2.report_month = list.month_end_num
    WHERE budget_month1.total_amnt IS NOT NULL 
        AND budget_month2.total_amnt IS NOT NULL) 

, calculate_weekly_budget AS ( 
    SELECT 
       acct_no 
       , acct_title
       , dept_no 
       , dep_title
       , class_id
       , class_name
       , week_start_date
       , week_end_date
       , budget_month_start
       , budget_month_end
       , days_in_month_start_in_week
       , days_in_month_end_in_week
       , start_month_days
       , end_month_days
       , ROUND((
            budget_month_start::NUMERIC / start_month_days * days_in_month_start_in_week
                + budget_month_end::NUMERIC / end_month_days * days_in_month_end_in_week
        ), 2 ) AS weekly_budget
    FROM add_budget
)   

SELECT * 
FROM calculate_weekly_budget