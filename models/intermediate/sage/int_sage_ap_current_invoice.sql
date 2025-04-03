WITH ap_bill AS (
    SELECT * FROM {{ ref('stg_sage_ap_bill')}}
)

SELECT 
record_num
, invoice_num
, vendor_id 
, vendor_name
, mega_entity_key
, mega_entity_name
, record_url
, ship_to_return_to_key
, discount_date
, posted_date
, invoice_date
, sage_rec_payment_date
, term_name
, due_date
, paid_date
, state 
, total_paid 
, total_due 
, modified_date
, sage_submitted_date
FROM ap_bill
WHERE 
    lower(state)!='draft'
    AND _fivetran_deleted = false