WITH dependent_deduction AS (
    SELECT * FROM {{ source('ukg', 'dependent_deduction') }}
)

SELECT
    CAST(id AS VARCHAR) AS id,
    CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted,
    CAST(company_id AS VARCHAR) AS company_id,
    CAST(contact_id AS VARCHAR) AS contact_id,
    CAST(employee_id AS VARCHAR) AS employee_id,
    CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced,
    CAST(code AS VARCHAR) AS code,
    CAST(dep_b_plan_tv_id AS INTEGER) AS dep_b_plan_tv_id,
    CAST(need_eoi AS BOOLEAN) AS need_eoi,
    CAST(type AS VARCHAR) AS type,
    CAST(benefit_stop_date AS TIMESTAMP) AS benefit_stop_date,
    CAST(benefit_status AS VARCHAR) AS benefit_status,
    CAST(benefit_start_date AS TIMESTAMP) AS benefit_start_date,
    CAST(is_benefit_waived AS BOOLEAN) AS is_benefit_waived,
    CAST(declined_by_carrier AS VARCHAR) AS declined_by_carrier,
    CAST(benefit_status_date AS TIMESTAMP) AS benefit_status_date,
    CAST(date_time_changed AS TIMESTAMP) AS date_time_changed,
    CAST(benefit_amount AS DOUBLE PRECISION) AS benefit_amount,
    CAST(current_co_id AS VARCHAR) AS current_co_id,
    CAST(date_time_created AS TIMESTAMP) AS date_time_created
FROM dependent_deduction


