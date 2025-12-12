-- This maps digital chalk registration users to their respective users with user name, email. This email would 
-- be used to match with the employee email in the UKG system.

WITH dc_registrations AS (
    SELECT * FROM {{ ref('stg_dc_registrations') }}
)

, dc_users AS (
    SELECT * FROM {{ ref('stg_dc_users') }}
)

, full_ukg_employee_list AS (
    SELECT * FROM {{ ref('ukg_full_employee_list') }}
)

, entra_users AS (
    SELECT * FROM {{ ref('stg_entra_users') }}
)

, entra_w_ukg AS (
    SELECT 
        dc.*, 
        entra.employee_id, 
        COALESCE(entra.job_title, employee_list.job_description) AS job_title, 
        COALESCE(entra.company_name, employee_list.site_description) AS company_name, 
        employee_list.work_location
    FROM public.data_digital_chalk_users dc
    LEFT JOIN public.entra_users entra
        ON lower(dc.username)= lower(user_principal_name)
    LEFT JOIN dbt_analytics.ukg_full_employee_list employee_list 
        ON employee_list.employment_id = entra.employee_id
)     

SELECT 
    DISTINCT
    r.begin_date
    , r.end_date
    , r.grade
    , r.last_active_date
    , r.offering_id
    , r.title
    , r.status
    , r.created_date
    , r.dc_registration_id
    , r.dc_user_id
    , u.employee_id AS ukg_employee_id
	, u.first_name AS dc_first_name
    , u.last_name AS dc_last_name
    , u.username AS dc_username
    , u.email AS dc_email
    , u.tags AS tags 
    , u.locale AS dc_locale
    , u.last_login_date AS user_last_login_date
	, u.job_title AS job_title
	, u.company_name AS company_name
    , u.work_location AS work_location
FROM dc_registrations r
LEFT JOIN entra_w_ukg u 
	ON r.dc_user_id = u.dc_user_id
