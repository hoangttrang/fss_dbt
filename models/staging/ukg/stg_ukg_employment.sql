WITH employment AS (
    SELECT * FROM  {{ source ('ukg', 'employment') }}
)


SELECT
    CAST(id AS VARCHAR) AS id
    , CAST(date_time_changed AS TIMESTAMP) AS date_time_changed
    , CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted
    , CAST(employee_id AS VARCHAR) AS employee_id
    , CAST(organization_level_1_id AS VARCHAR) AS organization_level_1_id
    , CAST(organization_level_2_id AS VARCHAR) AS organization_level_2_id
    , CAST(organization_level_3_id AS VARCHAR) AS organization_level_3_id
    , CAST(organization_level_4_id AS VARCHAR) AS organization_level_4_id
    , CAST(pay_group AS VARCHAR) AS pay_group
    , CAST(primary_job_id AS VARCHAR) AS primary_job_id
    , CAST(primary_work_location_id AS VARCHAR) AS primary_work_location_id
    , CAST(supervisor_id AS VARCHAR) AS supervisor_id
    , CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced
    , CAST(is_multiple_job AS VARCHAR) AS is_multiple_job
    , CAST(shift AS VARCHAR) AS shift
    , CAST(supervisor_first_name AS VARCHAR) AS supervisor_first_name
    , CAST(leave_reason_code AS VARCHAR) AS leave_reason_code
    , CAST(date_last_worked AS TIMESTAMP) AS date_last_worked
    , CAST(supervisor_last_name AS VARCHAR) AS supervisor_last_name
    , CAST(is_autopaid AS VARCHAR) AS is_autopaid
    , CAST(date_in_job AS TIMESTAMP) AS date_in_job
    , CAST(deduction_group_code AS VARCHAR) AS deduction_group_code
    , CAST(language_code AS VARCHAR) AS language_code
    , CAST(date_last_pay_date_paid AS TIMESTAMP) AS date_last_pay_date_paid
    , CAST(status_start_date AS TIMESTAMP) AS status_start_date
    , CAST(date_of_seniority AS TIMESTAMP) AS date_of_seniority
    , CAST(scheduled_annual_hrs AS DOUBLE PRECISION) AS scheduled_annual_hrs
    , CAST(salary_or_hourly AS VARCHAR) AS salary_or_hourly
    , CAST(job_title AS VARCHAR) AS job_title
    , CAST(hire_source AS VARCHAR) AS hire_source
    , CAST(scheduled_work_hrs AS DOUBLE PRECISION) AS scheduled_work_hrs
    , CAST(shift_group AS VARCHAR) AS shift_group
    , CAST(date_of_benefit_seniority AS TIMESTAMP) AS date_of_benefit_seniority
    , CAST(date_of_retirement AS TIMESTAMP) AS date_of_retirement
    , CAST(earning_group_code AS VARCHAR) AS earning_group_code
    , CAST(job_description AS VARCHAR) AS job_description
    , CAST(termination_reason_description AS VARCHAR) AS termination_reason_description
    , CAST(job_change_reason_code AS VARCHAR) AS job_change_reason_code
    , CAST(employee_status_code AS VARCHAR) AS employee_status_code
    , CAST(primary_project_code AS VARCHAR) AS primary_project_code
    , CAST(pay_group_description AS VARCHAR) AS pay_group_description
    , CAST(supervisor_company_code AS VARCHAR) AS supervisor_company_code
    , CAST(employee_type_code AS VARCHAR) AS employee_type_code
    , CAST(date_of_early_retirement AS TIMESTAMP) AS date_of_early_retirement
    , CAST(work_phone_country AS VARCHAR) AS work_phone_country
    , CAST(term_reason AS VARCHAR) AS term_reason
    , CAST(is_auto_allocated AS VARCHAR) AS is_auto_allocated
    , CAST(supervisor_co_id AS VARCHAR) AS supervisor_co_id
    , CAST(last_hire_date AS TIMESTAMP) AS last_hire_date
    , CAST(work_phone_extension AS VARCHAR) AS work_phone_extension
    , CAST(pay_period AS VARCHAR) AS pay_period
    , CAST(full_time_or_part_time_code AS VARCHAR) AS full_time_or_part_time_code
    , CAST(weekly_hours AS DOUBLE PRECISION) AS weekly_hours
    , CAST(original_hire_date AS TIMESTAMP) AS original_hire_date
    , CAST(date_paid_thru AS TIMESTAMP) AS date_paid_thru
    , CAST(auto_allocate AS VARCHAR) AS auto_allocate
    , CAST(ok_to_rehire AS VARCHAR) AS ok_to_rehire
    , CAST(scheduled_fte AS DOUBLE PRECISION) AS scheduled_fte
    , CAST(date_of_termination AS TIMESTAMP) AS date_of_termination
    , CAST(date_time_created AS TIMESTAMP) AS date_time_created
    , CAST(term_type AS VARCHAR) AS term_type
    , CAST(planned_leave_reason AS BIGINT) AS planned_leave_reason
    , CAST(company_id AS VARCHAR) AS company_id
    , CAST(job_group_code AS VARCHAR) AS job_group_code
FROM employment