WITH ap_bill_tab AS ( 
    SELECT * FROM {{ ref('int_sage_ap_current_invoice') }}
)

, ap_bill_item AS (
    SELECT *
    FROM {{ ref('stg_sage_ap_bill_item') }}
)


SELECT 
    bill.record_num
    , bill.invoice_num
    , COALESCE(bill.vendor_id::text, '') || ' - ' || COALESCE(bill.vendor_name, '') AS vendor_name_full
    , bill.vendor_name 
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
    , bill_item.line_item_state
    , bill_item.line_no
    , bill.record_url
FROM ap_bill_tab AS bill
LEFT JOIN ap_bill_item AS bill_item 
    ON bill.record_num = bill_item.record_num
    AND bill.invoice_num = bill_item.invoice_num
    AND bill.vendor_id = bill_item.vendor_id