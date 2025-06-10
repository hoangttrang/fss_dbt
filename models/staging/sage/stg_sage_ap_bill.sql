WITH ap_bill AS (
    SELECT * FROM  {{ source ('sage', 'ap_bill') }}
)

SELECT 
    recordno AS record_num
    , megaentityname AS mega_entity_name
    , megaentitykey AS mega_entity_key
    , megaentityid AS mega_entity_id
    , vendorid AS vendor_id 
    , vendorname AS vendor_name
    , paymentpriority AS payment_priority
    , recordid AS invoice_num
    , state  
    , userid AS user_id
    , termvalue AS term_value
    , termkey AS term_key
    , termname AS term_name
    , rawstate AS raw_state
    , whendiscount AS discount_date
    , whencreated AS invoice_date
    , whenposted AS posted_date
    , whenmodified AS modified_date
    , whendue AS due_date
    , whenpaid AS paid_date
    , recpaymentdate AS sage_rec_payment_date
    , totalpaid AS total_paid 
    , totaldue AS total_due
    , description 
    , modifiedby AS modified_by
    , createdby AS created_by
    , createduserid AS created_user_id
    , currency AS currency
    , basecurr AS base_currency
    , trx_totalpaid AS trx_total_paid
    , trx_totaldue AS trx_total_due
    , trx_totalretained AS trx_total_retained
    , trx_totalentered AS trx_total_entered
    , trx_totalreleased AS trx_total_released
    , trx_totalselected AS trx_total_selected
    , trx_entitydue AS trx_entity_due
    , supdocid AS sup_doc_id
    , docnumber AS doc_number
    , docsource AS doc_source
    , recordtype AS record_type
    , systemgenerated AS system_generated
    , auwhencreated AS sage_submitted_date
    , due_in_days
    , record_url 
    , retainagepercentage AS retainage_percentage
    , retainagereleased AS retainage_released
    , onhold AS on_hold
    , uploadstatus AS upload_status
    , inclusivetax AS inclusive_tax
    , totalretained AS total_retained
    , totalentered AS total_entered 
    , totalselected AS total_selected
    , shiptoreturntokey AS ship_to_return_to_key
    , billtopaytokey AS bill_to_pay_to_key
    , modulekey AS module_key
    , shiptoreturntocontactname AS ship_to_return_to_contact_name
    , billtopaytocontactname AS bill_to_pay_to_contact_name
    , prbatchkey AS pr_batch_key
    , prbatch AS pr_batch
    , form_1099_box
    , form_1099_type AS form_1099_type
    , schopkey 
    , _fivetran_synced
    , _fivetran_deleted
    
FROM ap_bill