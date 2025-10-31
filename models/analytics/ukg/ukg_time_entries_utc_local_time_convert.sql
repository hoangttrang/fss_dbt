WITH employee AS(
    SELECT * FROM {{ ref('stg_ukg_ready_employee') }}
)

, employment AS(
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, time_entries AS (
    SELECT * FROM {{ ref('stg_ukg_ready_time_entries') }}

)

, full_employee_list AS (
    SELECT 
        ukg_ready_employee.id AS row_id,
        full_employee_list.first_name,
        full_employee_list.last_name,
        site_id AS organization_level_4_id, 
        full_employee_list.employment_id,
        full_employee_list.employee_id, 
        full_employee_list.site_description,
        full_employee_list.job_description,
        full_employee_list.latest_hourly_pay_rate, 
        full_employee_list.job_family_code 
    FROM {{ ref('ukg_full_employee_list') }} full_employee_list
    INNER JOIN employee AS ukg_ready_employee
        ON ukg_ready_employee.employee_id = full_employee_list.employment_id
)

, motive_timezone AS (
    SELECT * FROM {{ ref ('consolidated_site_mapping_with_timezone')}}
)

, employee_full_timezone AS(
    SELECT 
        full_employee_list.*, 
        timezone
    FROM full_employee_list 
        LEFT JOIN motive_timezone 
            ON full_employee_list.organization_level_4_id = motive_timezone.ukg_location_id
)

, time_entries_transform AS (
    SELECT 
        account_id, 
        date, 
        total, 
        is_raw, 
        is_calc, 
        calc_start_time, 
        calc_end_time,
        calc_total, 
        approval_status, 
        MAX(entry_id) AS entry_id   
    FROM time_entries
    WHERE time_off_id IS NULL AND calc_total <> 0 and total is NOT NULL
    GROUP BY 
        account_id, 
        date, 
        total, 
        is_raw, 
        is_calc, 
        calc_start_time, 
        calc_end_time,
        calc_total, 
        approval_status
)

, pay_register AS (
    SELECT * FROM {{ ref ('stg_ukg_pay_register')}}
)

, pay_rate_window AS(
    SELECT 
        employment_id, 
        pay_date,
        MAX(hourly_pay_rate) AS hourly_pay_rate,
        pay_date - INTERVAL '11 days' AS pay_rate_start_date, 
        pay_date - INTERVAL '5 days' AS pay_rate_end_date
    FROM pay_register
    GROUP BY employment_id, pay_date
    ORDER BY employment_id, pay_date
)

, final_tab AS (
    SELECT 
        entry_id, 
        account_id, 
        employee_full_timezone.employee_id,
        CAST(employee_full_timezone.employment_id AS INT) AS employment_id,
        employee_full_timezone.first_name,
        employee_full_timezone.last_name,
        CONCAT(employee_full_timezone.first_name, ' ', employee_full_timezone.last_name) AS employee_full_name,
        employee_full_timezone.job_description,
        employee_full_timezone.job_family_code,
        employee_full_timezone.site_description,
        employee_full_timezone.organization_level_4_id AS ukg_location_id,
        employee_full_timezone.latest_hourly_pay_rate,
        pay_rate_window.hourly_pay_rate,
        CAST(date - (EXTRACT(DOW FROM date)::int - 1) * INTERVAL '1 day' AS date) AS timesheet_start,
        CAST(date - (EXTRACT(DOW FROM date)::int - 7) * INTERVAL '1 day' + INTERVAL '6 days' AS date) AS timesheet_end,
        date, 
        approval_status, 
        is_raw, 
        is_calc, 
        employee_full_timezone.timezone, 
        calc_start_time AS timesheet_start_time_utc,
        calc_start_time AT TIME ZONE employee_full_timezone.timezone AS timesheet_start_time_local, 
        calc_end_time AS timesheet_end_time_utc, 
        calc_end_time AT TIME ZONE employee_full_timezone.timezone AS timesheet_end_time_local,
        calc_total , 
        (CAST (calc_total AS FLOAT) /3600000) AS calc_total_hours, 
        to_char( (calc_total || ' milliseconds')::interval, 'HH24:MI' ) AS duration_hhmm,
        (CAST (calc_total AS FLOAT) /3600000) * employee_full_timezone.latest_hourly_pay_rate AS total_rate_amount
    FROM time_entries_transform
    LEFT JOIN employee_full_timezone 
        ON time_entries_transform.account_id = employee_full_timezone.row_id
    LEFT JOIN pay_rate_window
        ON employee_full_timezone.employment_id = pay_rate_window.employment_id
        AND date BETWEEN pay_rate_start_date AND pay_rate_end_date
    WHERE employee_full_timezone.organization_level_4_id IS NOT NULL 
    ) 

SELECT DISTINCT *
FROM final_tab