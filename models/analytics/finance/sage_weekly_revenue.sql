WITH sage_revenue AS (
    SELECT * FROM {{ ref('sage_revenue') }}
)

, date_map AS ( 
    SELECT DISTINCT * FROM {{ ref('int_dims_week_sun_to_sat') }}
)

, agg_revenue AS (
    SELECT 
        EXTRACT(YEAR FROM batch_date) AS report_year
        , EXTRACT(MONTH FROM batch_date) AS report_month
        , departmentid AS dept_no
        , departmenttitle AS dep_title
        , SUM(revenue) AS total_revenue
    FROM sage_revenue
    GROUP BY 1,2,3,4
)

, list_of_department_date AS (
    SELECT
        *
    FROM (SELECT DISTINCT dept_no, dep_title
        FROM agg_revenue)
    CROSS JOIN (SELECT * FROM date_map)
)

, add_revenue AS (
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
        , revenue_month1.total_revenue AS revenue_month_start
        , revenue_month2.total_revenue AS revenue_month_end
    FROM list_of_department_date AS list
    LEFT JOIN agg_revenue AS revenue_month1
    ON revenue_month1.dept_no = list.dept_no
        AND revenue_month1.report_year = list.year_start_num
        AND revenue_month1.report_month = list.month_start_num
    LEFT JOIN agg_revenue AS revenue_month2
    ON revenue_month2.dept_no = list.dept_no
        AND revenue_month2.report_year = list.year_end_num
        AND revenue_month2.report_month = list.month_end_num
    WHERE revenue_month1.total_revenue IS NOT NULL 
        AND revenue_month2.total_revenue IS NOT NULL
)

, calculate_weekly_revenue AS ( 
    SELECT 
       dept_no 
       , dep_title
       , week_start_date
       , week_end_date
       , start_month_days
       , end_month_days
       , ROUND((
            revenue_month_start::NUMERIC / start_month_days * days_in_month_start_in_week
                + revenue_month_end::NUMERIC / end_month_days * days_in_month_end_in_week
            ), 2 ) AS weekly_revenue
    FROM add_revenue
)

SELECT *
FROM calculate_weekly_revenue