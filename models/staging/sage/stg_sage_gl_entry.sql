-- General ledger ENTRY (line grain). Parent batch is gl_batch via
-- batchno = gl_batch.recordno. Like gl_detail, this is a hard-delete Fivetran
-- connector: Intacct deletes/voids do NOT propagate here, they only land on
-- gl_batch (_fivetran_deleted = true). Any spend/expense query built on this
-- model MUST gate on the parent batch's delete flag -- see the pattern in the
-- _sage__schema.yml description for stg_sage_gl_entry.
--
WITH gl_entry AS (
    SELECT * FROM {{ source('sage', 'gl_entry') }}
)

SELECT
    CAST(batchno AS character varying(64)) AS batchno,          -- join key: = gl_batch.recordno
    CAST(recordno AS character varying(64)) AS recordno,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(tr_type AS bigint) AS tr_type,
    CAST(accounttitle AS character varying(256)) AS accounttitle,
    CAST(adj AS boolean) AS adj,
    CAST(vendorname AS character varying(256)) AS vendorname,
    CAST(basecurr AS character varying(256)) AS basecurr,
    CAST(amount AS double precision) AS amount,
    CAST(batch_date AS date) AS batch_date,
    CAST(document AS character varying(256)) AS document,
    CAST(exch_rate_date AS date) AS exch_rate_date,
    CAST(accountkey AS bigint) AS accountkey,
    CAST(batch_number AS bigint) AS batch_number,
    CAST(clrdate AS date) AS clrdate,
    CAST(modifiedby AS bigint) AS modifiedby,
    CAST(exchange_rate AS bigint) AS exchange_rate,
    CAST(departmentkey AS bigint) AS departmentkey,
    CAST(department AS character varying(256)) AS department,
    CAST(record_url AS character varying(256)) AS record_url,
    CAST(trx_amount AS double precision) AS trx_amount,
    CAST(exch_rate_type_id AS bigint) AS exch_rate_type_id,
    CAST(line_no AS bigint) AS line_no,
    CAST(accountno AS bigint) AS accountno,
    CAST(location AS character varying(256)) AS location,
    CAST(whencreated AS timestamp with time zone) AS whencreated,
    CAST(state AS character varying(256)) AS state,               -- == gl_batch.state on 100% of rows (do NOT use for delete status)
    CAST(batchtitle AS character varying(1024)) AS batchtitle,
    CAST(classdimkey AS bigint) AS classdimkey,
    CAST(whenmodified AS timestamp with time zone) AS whenmodified,
    CAST(cleared AS character varying(256)) AS cleared,
    CAST(vendordimkey AS bigint) AS vendordimkey,
    CAST(billable AS boolean) AS billable,
    CAST(vendorid AS character varying(256)) AS vendorid,
    CAST(locationkey AS bigint) AS locationkey,
    CAST(description AS character varying(512)) AS description,
    CAST(statistical AS boolean) AS statistical,
    CAST(departmenttitle AS character varying(256)) AS departmenttitle,
    CAST(createdby AS bigint) AS createdby,
    CAST(classid AS bigint) AS classid,
    CAST(currency AS character varying(256)) AS currency,
    CAST(classname AS character varying(256)) AS classname,
    CAST(locationname AS character varying(256)) AS locationname,
    CAST(userno AS bigint) AS userno,
    CAST(entry_date AS date) AS entry_date,
    CAST(modifiedbyloginid AS character varying(256)) AS modifiedbyloginid,
    CAST(createdbyloginid AS character varying(256)) AS createdbyloginid,
    CAST(itemid AS bigint) AS itemid,
    CAST(itemname AS character varying(256)) AS itemname,
    CAST(itemdimkey AS bigint) AS itemdimkey
FROM gl_entry
