WITH data_digital_chalk_users AS (
    SELECT * FROM {{ source('digital_chalk', 'data_digital_chalk_users') }}
)

SELECT 
    CAST(dc_user_id AS character varying) AS dc_user_id,
    CAST(first_name AS character varying) AS first_name,
    CAST(last_name AS character varying) AS last_name,
    CAST(username AS character varying) AS username,
    CAST(email AS character varying) AS email,
    CAST(tags AS character varying[]) AS tags,
    CAST(locale AS character varying) AS locale,
    CAST(created_date AS timestamp without time zone) AS created_date,
    CAST(last_login_date AS timestamp without time zone) AS last_login_date,
    CAST(inserted_at AS timestamp without time zone) AS inserted_at,
    CAST(updated_at AS timestamp without time zone) AS updated_at
FROM data_digital_chalk_users