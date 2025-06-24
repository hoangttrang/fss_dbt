WITH earning_history_base_element AS (
    SELECT * FROM {{ source('ukg', 'earning_history_base_element') }}
)

SELECT
    CAST(_fivetran_id AS VARCHAR) AS _fivetran_id
    , CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted
    , CAST(company_id AS VARCHAR) AS company_id
    , CAST(earning_id AS VARCHAR) AS earning_id
    , CAST(employee_id AS VARCHAR) AS employee_id
    , CAST(employment_id AS VARCHAR) AS employment_id
    , CAST(job_id AS VARCHAR) AS job_id
    , CAST(location_id AS VARCHAR) AS location_id
    , CAST(pay_group AS VARCHAR) AS pay_group
    , CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced
    , CAST(include_in_deferred_compensation AS BOOLEAN) AS include_in_deferred_compensation
    , CAST(accrual_code AS VARCHAR) AS accrual_code
    , CAST(project AS VARCHAR) AS project
    , CAST(gen_number AS VARCHAR) AS gen_number
    , CAST(calculation_sequence AS BIGINT) AS calculation_sequence
    , CAST(job_premium_amount AS DOUBLE PRECISION) AS job_premium_amount
    , CAST(number_of_games AS INTEGER) AS number_of_games
    , CAST(base_amount AS DOUBLE PRECISION) AS base_amount
    , CAST(piece_pay_rate AS DOUBLE PRECISION) AS piece_pay_rate
    , CAST(period_control AS BIGINT) AS period_control
    , CAST(time_clock_code AS VARCHAR) AS time_clock_code
    , CAST(gross_up_target AS DOUBLE PRECISION) AS gross_up_target
    , CAST(is_voiding_record AS BOOLEAN) AS is_voiding_record
    , CAST(piece_count AS DOUBLE PRECISION) AS piece_count
    , CAST(hourly_pay_rate AS DOUBLE PRECISION) AS hourly_pay_rate
    , CAST(calculation_rule AS VARCHAR) AS calculation_rule
    , CAST(tip_credit AS DOUBLE PRECISION) AS tip_credit
    , CAST(is_voided AS BOOLEAN) AS is_voided
    , CAST(tax_calculation_group_id AS VARCHAR) AS tax_calculation_group_id
    , CAST(pay_rate AS DOUBLE PRECISION) AS pay_rate
    , CAST(report_category AS VARCHAR) AS report_category
    , CAST(pay_date AS TIMESTAMP) AS pay_date
    , CAST(include_in_deferred_compensation_hours AS BOOLEAN) AS include_in_deferred_compensation_hours
    , CAST(period_pay_rate AS DOUBLE PRECISION) AS period_pay_rate
    , CAST(current_hours AS DOUBLE PRECISION) AS current_hours
    , CAST(tip_type AS VARCHAR) AS tip_type
    , CAST(job_premium_rate_or_percent AS DOUBLE PRECISION) AS job_premium_rate_or_percent
    , CAST(use_deduction_off_set AS BOOLEAN) AS use_deduction_off_set
    , CAST(number_of_days AS INTEGER) AS number_of_days
    , CAST(gross_up AS VARCHAR) AS gross_up
    , CAST(ytd_shift_amount AS DOUBLE PRECISION) AS ytd_shift_amount
    , CAST(tax_category AS VARCHAR) AS tax_category
    , CAST(gross_up_tax_calculation_method AS INTEGER) AS gross_up_tax_calculation_method
    , CAST(tip_gross_receipts AS DOUBLE PRECISION) AS tip_gross_receipts
    , CAST(current_amount AS DOUBLE PRECISION) AS current_amount
    , CAST(gl_follow_base_account_allocation AS VARCHAR) AS gl_follow_base_account_allocation
    , CAST(payout_rate_type AS VARCHAR) AS payout_rate_type
    , CAST(ytd_amount AS DOUBLE PRECISION) AS ytd_amount
FROM earning_history_base_element