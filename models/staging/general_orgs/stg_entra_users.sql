WITH entra_users AS (
    SELECT * FROM {{ source('general_orgs', 'entra_users') }}
)


SELECT 
    CAST(entra_user_id AS character varying) AS entra_user_id,
    CAST(employee_id AS character varying) AS employee_id,
    CAST(display_name AS character varying) AS display_name,
    CAST(first_name AS character varying) AS first_name,
    CAST(last_name AS character varying) AS last_name,
    CAST(user_type AS character varying) AS user_type,
    CAST(user_principal_name AS character varying) AS user_principal_name,
    CAST(job_title AS character varying) AS job_title,
    CAST(department AS character varying) AS department,
    CAST(company_name AS character varying) AS company_name,
    CAST(employee_type AS character varying) AS employee_type,
    CAST(mail_nickname AS character varying) AS mail_nickname,
    CAST(email AS character varying) AS email,
    CAST(other_email AS character varying) AS other_email,
    CAST(is_active AS boolean) AS is_active,
    CAST(updated_at AS timestamp without time zone) AS updated_at
FROM entra_users

