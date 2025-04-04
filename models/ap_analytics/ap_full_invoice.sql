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
    , bill.invoice_date
    , bill.posted_date
    , bill.modified_date
    , bill.due_date
    , bill.paid_date
    , bill.state 
    , bill.total_paid AS inv_total_paid
    , bill.total_due AS inv_total_due
    , bill.sage_submitted_date
    , COALESCE(bill_item.account_no::text, '') || ' - ' || COALESCE(bill_item.account_title, '') AS account_name_full
    , COALESCE(bill_item.class_id::text, '') || ' - ' || COALESCE(bill_item.class_name, '') AS class_name_full
    , bill_item.class_name
    , bill_item.class_id
    , COALESCE(bill_item.department_id::text, '') || ' - ' || COALESCE(bill_item.department_name, '') AS department_name_full
    , bill_item.line_item_states
    , bill_item.total_line_items
    , bill.record_url
    , CASE
        WHEN bill_item.record_num IS NULL AND bill.state = 'Paid' THEN bill.total_paid
        WHEN bill_item.record_num IS NOT NULL  AND bill.state = 'Paid' THEN bill_item.amount
        ELSE 0
    END AS total_line_paid

    -- New total_due column
    , CASE
        WHEN bill_item.record_num IS NOT NULL AND bill.state = 'Paid' THEN 0
        WHEN bill_item.record_num IS NULL AND bill.state <> 'Paid' THEN bill.total_due
        WHEN bill_item.record_num IS NOT NULL  AND bill.state <> 'Paid'  THEN bill_item.amount
        ELSE 0
    END AS total_line_due
FROM ap_bill_tab AS bill
LEFT JOIN ap_bill_item AS bill_item 
    ON bill.record_num = bill_item.record_num
    AND bill.invoice_num = bill_item.invoice_num
    AND bill.vendor_id = bill_item.vendor_id

-- basically, the logic here is that if there is no match in bill_item, we would have to use total_paid and total_bill; 
-- if there is a match in bill_item, we want to use amount column from bill_item 