WITH location AS (
    SELECT * FROM {{ source ('ukg', 'location') }}
)

SELECT 
    CAST(id AS character varying(256)) AS id,
    CAST(_fivetran_deleted AS boolean) AS _fivetran_deleted,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(country_code AS character varying(256)) AS country_code,
    CAST(is_active AS boolean) AS is_active,
    CAST(city AS character varying(256)) AS city,
    CAST(address_line_1 AS character varying(256)) AS address_line_1,
    CAST(description AS character varying(256)) AS description,
    CAST(address_line_2 AS character varying(256)) AS address_line_2,
    CAST(state AS character varying(256)) AS state,
    CAST(location_gl_segment AS character varying(256)) AS location_gl_segment,
    CAST(zip_or_postal_code AS bigint) AS zip_or_postal_code
FROM location
