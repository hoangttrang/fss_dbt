WITH employee_status AS (
    SELECT * FROM  {{ source ('ukg', 'employee_status') }}
)

SELECT
    CAST(company_id AS VARCHAR) AS company_id
    , CAST(employee_id AS VARCHAR) AS employee_id
    , CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted
    , CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced
    , CAST(status_start_date AS TIMESTAMP) AS status_start_date
    , CAST(as_of_date AS TIMESTAMP) AS as_of_date
    , CAST(trigger_termination AS BOOLEAN) AS trigger_termination
    , CAST(is_primary AS BOOLEAN) AS is_primary
    , CAST(status_reason AS VARCHAR) AS status_reason
    , CAST(status_reason_desc AS VARCHAR) AS status_reason_desc
    , CAST(status AS VARCHAR) AS status
FROM employee_status 

