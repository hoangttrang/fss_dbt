WITH ap_full_invoice AS (
    SELECT * FROM {{ ref('ap_full_invoice') }}
)

SELECT  
    vendor_name_full
    , vendor_name
    , department_name_full
    , department_name
    , account_name_full
    , account_name
    , class_name_full
    , invoice_num
    , record_url
    , posted_date
    , due_date
    , invoice_date
    , sage_submitted_date
    , total_due
    , CURRENT_DATE - sage_submitted_date::date AS stale_submitted_age
    , CASE
        WHEN CURRENT_DATE - sage_submitted_date::date <= 5 THEN '0-5'
        WHEN CURRENT_DATE - sage_submitted_date::date <= 10 THEN '5-10'
        WHEN CURRENT_DATE - sage_submitted_date::date <= 15 THEN '10-15'
        ELSE '15+'
    END AS stale_bucket
FROM ap_fulL_invoice 
WHERE state = 'Submitted'
