WITH employee AS(
    SELECT * FROM {{ ref('stg_ukg_ready_employee') }}
)

, employment AS(
        SELECT * FROM {{ ref('stg_ukg_employment') }}
)

, employ_full AS(
    SELECT employee.id AS row_id, employee.employee_id, employment.id, employment.organization_level_4_id
    FROM employee
        LEFT JOIN employment ON employee.employee_id = employment.id
)

,motive_timezone AS (
    SELECT * FROM {{ ref ('consolidated_site_mapping_with_timezone')}}
)

, employ_full_timezone AS(
    SELECT employ_full.row_id, employ_full.organization_level_4_id, timezone
    FROM employ_full
        LEFT JOIN motive_timezone ON employ_full.organization_level_4_id = motive_timezone.ukg_location_id
)

, time_entries AS (
    SELECT * FROM {{ ref('stg_ukg_ready_time_entries') }}
)


SELECT entry_id, account_id, start_date, end_date, date, 
	start_time, start_time AT TIME ZONE timezone AS start_time_local, 
	end_time, end_time AT TIME ZONE timezone AS end_time_local,
	total, approval_status, is_raw, is_calc, 
	calc_start_time, calc_start_time AT TIME ZONE timezone AS calc_start_time_local, 
	calc_end_time, calc_end_time AT TIME ZONE timezone AS calc_end_time_local,
	calc_total, piecework, amount, custom_decimal, time_off_id, cost_center_ids
	
FROM time_entries
	LEFT JOIN employ_full_timezone ON time_entries.account_id = employ_full_timezone.row_id