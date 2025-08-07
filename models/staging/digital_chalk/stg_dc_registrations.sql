WITH data_digital_chalk_registrations AS (
    SELECT * FROM {{ source('digital_chalk', 'data_digital_chalk_registrations') }}
)

SELECT 
    CAST(dc_registration_id AS character varying) AS dc_registration_id,
    CAST(dc_user_id AS character varying) AS dc_user_id,
    CAST(begin_date AS timestamp without time zone) AS begin_date,
    CAST(created_date AS timestamp without time zone) AS created_date,
    CAST(end_date AS timestamp without time zone) AS end_date,
    CAST(grade AS character varying) AS grade,
    CAST(last_active_date AS timestamp without time zone) AS last_active_date,
    CAST("offeringId" AS character varying) AS offeringId,
    CAST(title AS character varying) AS title,
    CAST(status AS character varying) AS status,
    CAST(inserted_at AS timestamp without time zone) AS inserted_at,
    CAST(updated_at AS timestamp without time zone) AS updated_at
FROM data_digital_chalk_registrations