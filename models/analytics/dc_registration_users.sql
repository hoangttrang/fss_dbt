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


SELECT 
    r.*
	, u.first_name AS dc_first_name
    , u.last_name AS dc_last_name
    , u.username AS dc_username
    , u.email AS dc_email
    , u.tags AS tags 
    , u.locale AS dc_locale
    , u.created_date AS user_created_date
    , u.last_login_date AS user_last_login_date
	, employee_list.site_id AS region
	, employee_list.site_description AS site
	, employee_list.employment_id
FROM dc_registrations r
INNER JOIN dc_users u 
	ON r.dc_user_id = u.dc_user_id
LEFT JOIN full_ukg_employee_list employee_list	
	ON LOWER(u.email) = LOWER(employee_list.email_address)