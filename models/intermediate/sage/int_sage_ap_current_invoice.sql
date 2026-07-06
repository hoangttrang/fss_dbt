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
    -- NULL-safe: Fivetran leaves _fivetran_deleted NULL on never-deleted rows,
    -- so a bare `= false` would silently drop every live bill.
    AND COALESCE(_fivetran_deleted, FALSE) = FALSE
    -- A bill voided in Sage can survive in ap_bill with full totaldue and the
    -- delete flag unset; the zero-out lives only on its entry batch.
    -- Join key: ap_bill.prbatchkey = ap_bill_batch.recordno (BIGINT -> STRING).
    -- NOT EXISTS (drop only on positive evidence): a present, live batch whose
    -- total rounds to exactly 0. A missing batch is kept ("unknown -> keep"),
    -- because ap_bill_batch is under-synced.
    AND NOT EXISTS (
        SELECT 1 FROM {{ ref('stg_sage_ap_bill_batch') }} bt
        WHERE bt.recordno = CAST(pr_batch_key AS character varying)
          AND COALESCE(bt._fivetran_deleted, FALSE) = FALSE
          AND CAST(bt.total AS DECIMAL(19,4)) = 0
    )