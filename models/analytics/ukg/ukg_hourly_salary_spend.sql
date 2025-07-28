WITH ukg_full_employee_list AS (
    SELECT * FROM {{ ref('ukg_full_employee_list') }}
)

, ukg_weekly_payment_salary AS (
    SELECT * FROM {{ ref('ukg_weekly_payment_salary') }}
)

,  date_series AS (
    SELECT generate_series('2021-01-01'::date, CURRENT_DATE, '1 day') AS active_date
)

, daily_active_employees AS (
    SELECT
        ds.active_date, 
        e.site_description,
        e.job_family_code,
        COUNT(e.employee_id) AS active_employee_count
    FROM
        date_series ds
    LEFT JOIN
        ukg_full_employee_list e
        ON e.original_hire_date <= ds.active_date
        AND (e.date_of_termination IS NULL OR e.date_of_termination > ds.active_date)
    GROUP BY
        ds.active_date, 
        e.site_description,
        e.job_family_code
)

, weekly_employee_avg AS (
    SELECT
        EXTRACT(YEAR FROM active_date) AS year,
        EXTRACT(WEEK FROM active_date) AS week_number,
        site_description,
        job_family_code,
        AVG(active_employee_count) AS avg_weekly_headcount
    FROM daily_active_employees
    GROUP BY
        EXTRACT(YEAR FROM active_date),
        EXTRACT(WEEK FROM active_date),
        site_description,
        job_family_code
)


, salary_data AS (
    SELECT
        EXTRACT(YEAR FROM pay_date) AS year,
        EXTRACT(WEEK FROM pay_date) AS week_number,
        site_description, 
        job_family_code,
        SUM(total_weekly_salary) as total_weekly_salary
    FROM
        ukg_weekly_payment_salary
    GROUP BY
        EXTRACT(YEAR FROM pay_date),
        EXTRACT(WEEK FROM pay_date),
        site_description,
        job_family_code

)

, weekly_joined AS (
    SELECT
        s.year,
        s.week_number,
        s.site_description,
        s.job_family_code,
        s.total_weekly_salary,
        e.avg_weekly_headcount,
        CASE 
            WHEN e.avg_weekly_headcount > 0 THEN s.total_weekly_salary / (40 * e.avg_weekly_headcount)
            ELSE NULL
        END AS avg_hourly_salary
    FROM
        salary_data s
    LEFT JOIN
        weekly_employee_avg e
        ON s.year = e.year
        AND s.week_number = e.week_number
        AND s.site_description = e.site_description
        AND s.job_family_code = e.job_family_code
)


SELECT
    year,
    site_description,
    job_family_code,
    AVG(avg_hourly_salary) AS avg_hourly_salary_yearly
FROM
    weekly_joined
GROUP BY
    year,
    site_description,
    job_family_code
ORDER BY
    year DESC,
    site_description,
    job_family_code;