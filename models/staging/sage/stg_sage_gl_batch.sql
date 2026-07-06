WITH gl_batch AS (
    SELECT * FROM {{ source('sage', 'gl_batch') }}
)

SELECT
    CAST(recordno AS character varying) AS recordno,
    CAST(_fivetran_deleted AS boolean) AS _fivetran_deleted,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced
FROM gl_batch
