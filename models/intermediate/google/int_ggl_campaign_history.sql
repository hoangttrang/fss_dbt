WITH ggl_campaign_history AS (
    SELECT * 
    FROM {{ ref('stg_ggl_ads_campaign_history') }}

)

, fields AS (
    SELECT
        advertising_channel_subtype
        , advertising_channel_type
        , customer_id
        , end_date
        , id
        , name
        , serving_status
        , start_date
        , status
        , tracking_url_template
        , updated_at
        , CAST(NULL AS BOOLEAN) AS _fivetran_active
        , CAST('' AS VARCHAR) AS source_relation
    FROM ggl_campaign_history
)

SELECT
    source_relation
    , id AS campaign_id
    , updated_at
    , name AS campaign_name
    , customer_id AS account_id
    , advertising_channel_type
    , advertising_channel_subtype
    , start_date
    , end_date
    , serving_status
    , status
    , tracking_url_template
    , ROW_NUMBER() OVER (PARTITION BY source_relation, id ORDER BY updated_at DESC) = 1 AS is_most_recent_record
FROM fields
WHERE COALESCE(_fivetran_active, TRUE)