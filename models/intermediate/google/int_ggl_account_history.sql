WITH ggl_account_history AS (
    SELECT * 
    FROM {{ ref('stg_ggl_ads_account_history') }}
)

, fields AS (
    SELECT
        _fivetran_synced
        , auto_tagging_enabled
        , currency_code
        , descriptive_name
        , id
        , time_zone
        , updated_at
        , CAST(NULL AS BOOLEAN) AS _fivetran_active
        , CAST('' AS VARCHAR) AS source_relation
    FROM ggl_account_history
)

SELECT
    source_relation
    , id AS account_id
    , updated_at
    , currency_code
    , auto_tagging_enabled
    , time_zone
    , descriptive_name AS account_name
    , ROW_NUMBER() OVER (PARTITION BY source_relation, id ORDER BY updated_at DESC) = 1 AS is_most_recent_record
FROM fields
WHERE COALESCE(_fivetran_active, TRUE)
