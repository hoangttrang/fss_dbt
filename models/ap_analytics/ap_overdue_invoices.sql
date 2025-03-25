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
    , CASE
        WHEN CURRENT_DATE - due_date <= 5 THEN '0-5'
        WHEN CURRENT_DATE - due_date <= 10 THEN '5-10'
        WHEN CURRENT_DATE - due_date <= 15 THEN '15-20'
        WHEN CURRENT_DATE - due_date <= 20 THEN '20-25'
        ELSE '25+'
    END AS overdue_bucket
    , invoice_num
    , record_url
    , state
    , invoice_date
    , due_date
    , modified_date
    , total_due
    , sage_submitted_date
FROM ap_full_invoice
WHERE total_due > 0 
   AND due_date <= CURRENT_DATE 
   AND state != 'Declined'
ORDER BY
    CASE
        WHEN CURRENT_DATE - due_date <= 5 THEN 1
        WHEN CURRENT_DATE - due_date <= 10 THEN 2
        WHEN CURRENT_DATE - due_date <= 15 THEN 3
        WHEN CURRENT_DATE - due_date <= 20 THEN 4
        ELSE 5
    END DESC,
    total_due DESC