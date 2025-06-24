WITH employee_change AS (
    SELECT * FROM  {{ source ('ukg', 'employee_change') }}
)

SELECT
    CAST(company_id AS VARCHAR) AS company_id
    , CAST(employee_id AS VARCHAR) AS employee_id
    , CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted
    , CAST(organization_level_1_id AS VARCHAR) AS organization_level_1_id
    , CAST(organization_level_2_id AS VARCHAR) AS organization_level_2_id
    , CAST(organization_level_3_id AS VARCHAR) AS organization_level_3_id
    , CAST(organization_level_4_id AS VARCHAR) AS organization_level_4_id
    , CAST(supervisor_id AS VARCHAR) AS supervisor_id
    , CAST(work_location_id AS VARCHAR) AS work_location_id
    , CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced
    , CAST(gender AS VARCHAR) AS gender
    , CAST(address_1 AS VARCHAR) AS address_1
    , CAST(prefix AS VARCHAR) AS prefix
    , CAST(work_phone AS VARCHAR) AS work_phone
    , CAST(supervisor_name AS VARCHAR) AS supervisor_name
    , CAST(date_of_last_hire AS VARCHAR) AS date_of_last_hire
    , CAST(suffix AS VARCHAR) AS suffix
    , CAST(type AS VARCHAR) AS type
    , CAST(zip_code AS BIGINT) AS zip_code
    , CAST(date_in_job AS VARCHAR) AS date_in_job
    , CAST(language_code AS VARCHAR) AS language_code
    , CAST(user_integration_key AS VARCHAR) AS user_integration_key
    , CAST(salary_or_hourly AS VARCHAR) AS salary_or_hourly
    , CAST(job_code AS VARCHAR) AS job_code
    , CAST(alternate_job_title AS VARCHAR) AS alternate_job_title
    , CAST(full_time_or_part_time AS VARCHAR) AS full_time_or_part_time
    , CAST(is_active AS BOOLEAN) AS is_active
    , CAST(project_code AS VARCHAR) AS project_code
    , CAST(hire_date AS VARCHAR) AS hire_date
    , CAST(alternate_email_address AS VARCHAR) AS alternate_email_address
    , CAST(termination_date AS VARCHAR) AS termination_date
    , CAST(empl_status_start_date AS VARCHAR) AS empl_status_start_date
    , CAST(status AS VARCHAR) AS status
    , CAST(job_group_code AS VARCHAR) AS job_group_code
    , CAST(employee_number AS VARCHAR) AS employee_number
FROM employee_change 