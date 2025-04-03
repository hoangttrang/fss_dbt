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
    , paid_date
    , due_date
    , invoice_date
    , sage_submitted_date
    , inv_total_paid
    , total_line_paid
FROM ap_fulL_invoice 
WHERE state = 'Paid'
