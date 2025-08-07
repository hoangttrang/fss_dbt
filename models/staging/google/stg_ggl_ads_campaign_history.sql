WITH campaign_history AS (
    SELECT * FROM {{ source('google_ads', 'campaign_history') }}
)

SELECT 
    CAST(id AS bigint) AS id,
    CAST(updated_at AS timestamp with time zone) AS updated_at,
    CAST(customer_id AS bigint) AS customer_id,
    CAST(base_campaign_id AS bigint) AS base_campaign_id,
    CAST(ad_serving_optimization_status AS character varying) AS ad_serving_optimization_status,
    CAST(advertising_channel_subtype AS character varying) AS advertising_channel_subtype,
    CAST(advertising_channel_type AS character varying) AS advertising_channel_type,
    CAST(experiment_type AS character varying) AS experiment_type,
    CAST(end_date AS character varying) AS end_date,
    CAST(final_url_suffix AS character varying) AS final_url_suffix,
    CAST(frequency_caps AS character varying) AS frequency_caps,
    CAST(name AS character varying) AS name,
    CAST(optimization_score AS double precision) AS optimization_score,
    CAST(payment_mode AS character varying) AS payment_mode,
    CAST(serving_status AS character varying) AS serving_status,
    CAST(start_date AS character varying) AS start_date,
    CAST(status AS character varying) AS status,
    CAST(vanity_pharma_display_url_mode AS character varying) AS vanity_pharma_display_url_mode,
    CAST(vanity_pharma_text AS character varying) AS vanity_pharma_text,
    CAST(video_brand_safety_suitability AS character varying) AS video_brand_safety_suitability,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(_fivetran_start AS timestamp with time zone) AS _fivetran_start,
    CAST(_fivetran_end AS timestamp with time zone) AS _fivetran_end,
    CAST(_fivetran_active AS boolean) AS _fivetran_active,
    CAST(tracking_url_template AS character varying) AS tracking_url_template
FROM campaign_history