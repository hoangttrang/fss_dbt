WITH employee AS (
    SELECT * FROM  {{ source ('ukg', 'employee') }}
)

SELECT
    CAST(id AS VARCHAR) AS id
    , CAST(date_time_changed AS TIMESTAMP) AS date_time_changed
    , CAST(_fivetran_deleted AS BOOLEAN) AS _fivetran_deleted
    , CAST(company_id AS VARCHAR) AS company_id
    , CAST(_fivetran_synced AS TIMESTAMPTZ) AS _fivetran_synced
    , CAST(address_latitude AS DOUBLE PRECISION) AS address_latitude
    , CAST(health_weight AS DOUBLE PRECISION) AS health_weight
    , CAST(i_9_doc_b AS VARCHAR) AS i_9_doc_b
    , CAST(i_9_doc_a AS VARCHAR) AS i_9_doc_a
    , CAST(military_is_disabled_vet AS VARCHAR) AS military_is_disabled_vet
    , CAST(ssn AS VARCHAR) AS ssn
    , CAST(cobra_is_active AS BOOLEAN) AS cobra_is_active
    , CAST(email_address_alternate AS VARCHAR) AS email_address_alternate
    , CAST(name_prefix_code AS VARCHAR) AS name_prefix_code
    , CAST(address_is_on_tax_boundary AS BOOLEAN) AS address_is_on_tax_boundary
    , CAST(i_9_verified AS BOOLEAN) AS i_9_verified
    , CAST(national_id AS VARCHAR) AS national_id
    , CAST(military_is_active_wartime_vet AS VARCHAR) AS military_is_active_wartime_vet
    , CAST(gender AS VARCHAR) AS gender
    , CAST(military_is_oth_elig_vet AS VARCHAR) AS military_is_oth_elig_vet
    , CAST(user_name AS VARCHAR) AS user_name
    , CAST(date_of_birth AS TIMESTAMP) AS date_of_birth
    , CAST(name_former AS VARCHAR) AS name_former
    , CAST(address_zip_code AS VARCHAR) AS address_zip_code
    , CAST(cobra_status AS VARCHAR) AS cobra_status
    , CAST(cobra_export AS VARCHAR) AS cobra_export
    , CAST(address_line_1 AS VARCHAR) AS address_line_1
    , CAST(ethnic_description AS VARCHAR) AS ethnic_description
    , CAST(address_line_2 AS VARCHAR) AS address_line_2
    , CAST(last_name AS VARCHAR) AS last_name
    , CAST(address_city AS VARCHAR) AS address_city
    , CAST(email_address AS VARCHAR) AS email_address
    , CAST(i_9_work_auth AS VARCHAR) AS i_9_work_auth
    , CAST(i_9_doc_c AS VARCHAR) AS i_9_doc_c
    , CAST(consent_electronic_w_2 AS BOOLEAN) AS consent_electronic_w_2
    , CAST(is_multi_pay_group AS BOOLEAN) AS is_multi_pay_group
    , CAST(ssn_is_suppressed AS BOOLEAN) AS ssn_is_suppressed
    , CAST(military_is_medal_vet AS VARCHAR) AS military_is_medal_vet
    , CAST(is_smoker AS BOOLEAN) AS is_smoker
    , CAST(marital_status_code AS VARCHAR) AS marital_status_code
    , CAST(former_name AS VARCHAR) AS former_name
    , CAST(person_id AS VARCHAR) AS person_id
    , CAST(home_phone AS BIGINT) AS home_phone
    , CAST(name_suffix_code AS VARCHAR) AS name_suffix_code
    , CAST(is_disabled AS BOOLEAN) AS is_disabled
    , CAST(home_phone_country AS VARCHAR) AS home_phone_country
    , CAST(address_id AS VARCHAR) AS address_id
    , CAST(sms_approvals AS BOOLEAN) AS sms_approvals
    , CAST(national_id_country AS VARCHAR) AS national_id_country
    , CAST(date_of_cobra_event AS TIMESTAMP) AS date_of_cobra_event
    , CAST(preferred_name AS VARCHAR) AS preferred_name
    , CAST(home_phone_is_private AS BOOLEAN) AS home_phone_is_private
    , CAST(last_name_not_same_as_ss_card AS VARCHAR) AS last_name_not_same_as_ss_card
    , CAST(address_state AS VARCHAR) AS address_state
    , CAST(military_separation_date AS TIMESTAMP) AS military_separation_date
    , CAST(ethnic_id_code AS VARCHAR) AS ethnic_id_code
    , CAST(consent_electronicw_2_pr AS BOOLEAN) AS consent_electronicw_2_pr
    , CAST(first_name AS VARCHAR) AS first_name
    , CAST(cobra_status_date AS TIMESTAMP) AS cobra_status_date
    , CAST(military_branch_served AS VARCHAR) AS military_branch_served
    , CAST(w_2_is_deceased AS BOOLEAN) AS w_2_is_deceased
    , CAST(cobra_reason AS VARCHAR) AS cobra_reason
    , CAST(address_country AS VARCHAR) AS address_country
    , CAST(military_service AS BOOLEAN) AS military_service
    , CAST(middle_name AS VARCHAR) AS middle_name
    , CAST(address_county AS VARCHAR) AS address_county
    , CAST(sms_pay_notification AS BOOLEAN) AS sms_pay_notification
    , CAST(date_time_created AS TIMESTAMP) AS date_time_created
    , CAST(origin_location AS VARCHAR) AS origin_location
    , CAST(date_deceased AS TIMESTAMP) AS date_deceased
    , CAST(disability_type AS VARCHAR) AS disability_type
    , CAST(military_era AS VARCHAR) AS military_era
    , CAST(nationality_1 AS VARCHAR) AS nationality_1
    , CAST(date_of_i_9_expiration AS TIMESTAMP) AS date_of_i_9_expiration
    , CAST(nationality_2 AS VARCHAR) AS nationality_2
    , CAST(community_broadcast_sms_code AS VARCHAR) AS community_broadcast_sms_code
    , CAST(origin_country AS VARCHAR) AS origin_country
FROM employee