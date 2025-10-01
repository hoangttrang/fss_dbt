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
FROM gl_detail
WHERE accounttitle ILIKE 'revenue%'
AND batch_state = 'P'
