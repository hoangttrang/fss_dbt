WITH ggl_campaign_stats AS (
    SELECT * 
    FROM {{ ref('stg_ggl_ads_campaign_stats') }}
)

, fields AS (
    SELECT
        _fivetran_id
        , _fivetran_synced
        , ad_network_type
        , clicks
        , cost_micros
        , customer_id
        , date
        , device
        , id
        , impressions
        , conversions
        , conversions_value
        , view_through_conversions
        , CAST('' AS VARCHAR) AS source_relation
    FROM ggl_campaign_stats
)

SELECT
    source_relation
    , customer_id AS account_id
    , date AS date_day
    , id AS campaign_id
    , ad_network_type
    , device
    , COALESCE(clicks, 0) AS clicks
    , COALESCE(cost_micros, 0) / 1000000.0 AS spend
    , COALESCE(impressions, 0) AS impressions
    , COALESCE(conversions, 0) AS conversions
    , COALESCE(conversions_value, 0) AS conversions_value
    , COALESCE(view_through_conversions, 0) AS view_through_conversions
FROM fields
