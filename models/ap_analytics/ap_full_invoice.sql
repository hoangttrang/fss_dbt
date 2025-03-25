WITH ap_bill_tab AS ( 
    SELECT * FROM {{ ref('int_sage_ap_current_invoice') }}
)

, ap_bill_item AS (
    SELECT *
    FROM {{ ref('stg_sage_ap_bill_item') }}
)

, joined_ap_bill_item AS (
    SELECT 
    bill.record_num,
    bill.invoice_num,
    bill.vendor_id,
    STRING_AGG(DISTINCT bill_item.account_no::text, ', ') AS account_no,
    STRING_AGG(DISTINCT bill_item.account_title, ', ') AS account_title,
    STRING_AGG(DISTINCT bill_item.class_id::text, ', ') AS class_id,
    STRING_AGG(DISTINCT bill_item.class_name, ', ') AS class_name,
    STRING_AGG(DISTINCT bill_item.department_id::text, ', ') AS department_id,
    STRING_AGG(DISTINCT bill_item.department_name, ', ') AS department_name,
    STRING_AGG(DISTINCT bill_item.line_item_state, ', ') AS line_item_states, 
    MAX(bill_item.line_no) AS total_line_items
    FROM ap_bill_tab AS bill
    LEFT JOIN ap_bill_item AS bill_item 
        ON bill.record_num = bill_item.record_num
        AND bill.invoice_num = bill_item.invoice_num
        AND bill.vendor_id = bill_item.vendor_id
    GROUP BY 
        bill.record_num,
        bill.invoice_num,
        bill.vendor_id
)

SELECT 
    bill.record_num
    , bill.invoice_num
    , COALESCE(bill.vendor_id::text, '') || ' - ' || COALESCE(bill.vendor_name, '') AS vendor_name_full
    , vendor_name 
    , bill.invoice_date
    , bill.posted_date
    , bill.modified_date
    , bill.due_date
    , bill.state 
    , bill.total_paid
    , bill.total_due
    , bill.sage_submitted_date
    , COALESCE(bill_item.account_no::text, '') || ' - ' || COALESCE(bill_item.account_title, '') AS account_name_full
    , account_title AS account_name
    , COALESCE(bill_item.class_id::text, '') || ' - ' || COALESCE(bill_item.class_name, '') AS class_name_full
    , COALESCE(bill_item.department_id::text, '') || ' - ' || COALESCE(bill_item.department_name, '') AS department_name_full
    , department_name
    , bill_item.line_item_states
    , bill_item.total_line_items
    , bill.record_url
FROM ap_bill_tab AS bill
LEFT JOIN joined_ap_bill_item AS bill_item 
    ON bill.record_num = bill_item.record_num
    AND bill.invoice_num = bill_item.invoice_num
    AND bill.vendor_id = bill_item.vendor_id