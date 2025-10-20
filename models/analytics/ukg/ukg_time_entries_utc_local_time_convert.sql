WITH employee AS(
    SELECT * FROM {{ ref('stg_ukg_ready_employee') }}
)

, employment AS(
    SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, employee_full AS(
    SELECT 
        employee.id AS row_id, 
        employee.employee_id, 
        employee.first_name,
        employee.last_name,
        employment.job_description, 
        employment.id, 
        employment.organization_level_4_id
    FROM employee
    LEFT JOIN employment 
        ON employee.employee_id = employment.id
)

, motive_timezone AS (
    SELECT * FROM {{ ref ('consolidated_site_mapping_with_timezone')}}
)

, employee_full_timezone AS(
    SELECT 
        employee_full.row_id, 
        employee_full.employee_id,
        employee_full.first_name,
        employee_full.last_name,
        employee_full.organization_level_4_id, 
        employee_full.job_description,
        timezone
    FROM employee_full
        LEFT JOIN motive_timezone 
            ON employee_full.organization_level_4_id = motive_timezone.ukg_location_id
)

, time_entries AS (
    SELECT * FROM {{ ref('stg_ukg_ready_time_entries') }}
)

SELECT 
    entry_id, 
    account_id, 
    employee_full_timezone.employee_id,
    employee_full_timezone.first_name,
    employee_full_timezone.last_name,
    employee_full_timezone.job_description,
    start_date, 
    end_date, 
    date, 
    organization_level_4_id AS ukg_location_id,
	start_time, 
    start_time AT TIME ZONE timezone AS start_time_local, 
	end_time, 
    end_time AT TIME ZONE timezone AS end_time_local,
	total, 
    approval_status, 
    is_raw, 
    is_calc, 
	calc_start_time, 
    employee_full_timezone.timezone, 
    calc_start_time AT TIME ZONE employee_full_timezone.timezone AS calc_start_time_local, 
	calc_end_time, 
    calc_end_time AT TIME ZONE employee_full_timezone.timezone AS calc_end_time_local,
    EXTRACT(EPOCH FROM (calc_end_time - calc_start_time))/3600 AS total_hours_calc,
    EXTRACT(EPOCH FROM (end_time - start_time))/3600 AS total_hours_norm,
	calc_total, 
    piecework, 
    amount, 
    custom_decimal, 
    time_off_id, 
    cost_center_ids
FROM time_entries
LEFT JOIN employee_full_timezone 
    ON time_entries.account_id = employee_full_timezone.row_id
WHERE employee_full_timezone.organization_level_4_id IS NOT NULL