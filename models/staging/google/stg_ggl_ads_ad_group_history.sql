WITH ad_group_history AS (
    SELECT * FROM {{ source('google_ads', 'ad_group_history') }}
)

SELECT 
    CAST(id AS bigint) AS id,
    CAST(updated_at AS timestamp with time zone) AS updated_at,
    CAST(campaign_id AS bigint) AS campaign_id,
    CAST(base_ad_group_id AS bigint) AS base_ad_group_id,
    CAST(ad_rotation_mode AS character varying) AS ad_rotation_mode,
    CAST(campaign_name AS character varying) AS campaign_name,
    CAST(display_custom_bid_dimension AS character varying) AS display_custom_bid_dimension,
    CAST(explorer_auto_optimizer_setting_opt_in AS boolean) AS explorer_auto_optimizer_setting_opt_in,
    CAST(final_url_suffix AS character varying) AS final_url_suffix,
    CAST(name AS character varying) AS name,
    CAST(status AS character varying) AS status,
    CAST(target_restrictions AS character varying) AS target_restrictions,
    CAST(tracking_url_template AS character varying) AS tracking_url_template,
    CAST(type AS character varying) AS type,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(_fivetran_start AS timestamp with time zone) AS _fivetran_start,
    CAST(_fivetran_end AS timestamp with time zone) AS _fivetran_end,
    CAST(_fivetran_active AS boolean) AS _fivetran_active
FROM ad_group_history