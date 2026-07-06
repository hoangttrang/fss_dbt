WITH ap_bill_batch AS (
    SELECT * FROM {{ source('sage', 'ap_bill_batch') }}
)

SELECT
    CAST(recordno AS character varying) AS recordno,
    CAST(total AS double precision) AS total,
    CAST(_fivetran_deleted AS boolean) AS _fivetran_deleted,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced
FROM ap_bill_batch
