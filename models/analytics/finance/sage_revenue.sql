WITH gl_detail AS (
    SELECT * FROM {{ ref('stg_sage_gl_detail') }}
)

SELECT 
    recordno,
    departmentid,
    departmenttitle,
    batch_date,
    batch_state,
    batch_no AS transaction_no,
    accounttitle,
    classname,
    description,
    creditamount AS revenue
FROM gl_detail d
WHERE accounttitle ILIKE 'revenue%'
AND batch_state = 'P'
-- Drop lines whose parent batch was soft-deleted in Intacct (deletes don't
-- propagate to gl_detail). Join key: gl_detail.batchkey = gl_batch.recordno (both STRING).
-- Semi-join: a line with NULL batchkey is an orphan and is dropped.
AND EXISTS (
    SELECT 1 FROM {{ ref('stg_sage_gl_batch') }} b
    WHERE b.recordno = d.batchkey
      AND COALESCE(b._fivetran_deleted, FALSE) = FALSE
)
