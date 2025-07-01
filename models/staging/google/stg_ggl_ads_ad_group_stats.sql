WITH ad_group_stats AS (
    SELECT * FROM {{ source('google_ads', 'ad_group_stats') }}
)

SELECT 
    CAST(customer_id AS bigint) AS customer_id,
    CAST(date AS date) AS date,
    CAST(_fivetran_id AS character varying) AS _fivetran_id,
    CAST(campaign_base_campaign AS character varying) AS campaign_base_campaign,
    CAST(conversions_value AS double precision) AS conversions_value,
    CAST(conversions AS double precision) AS conversions,
    CAST(interactions AS bigint) AS interactions,
    CAST(ad_network_type AS character varying) AS ad_network_type,
    CAST(interaction_event_types AS character varying) AS interaction_event_types,
    CAST(campaign_id AS bigint) AS campaign_id,
    CAST(impressions AS bigint) AS impressions,
    CAST(active_view_viewability AS double precision) AS active_view_viewability,
    CAST(id AS bigint) AS id,
    CAST(device AS character varying) AS device,
    CAST(view_through_conversions AS bigint) AS view_through_conversions,
    CAST(active_view_impressions AS bigint) AS active_view_impressions,
    CAST(clicks AS bigint) AS clicks,
    CAST(active_view_measurable_impressions AS bigint) AS active_view_measurable_impressions,
    CAST(active_view_measurable_cost_micros AS bigint) AS active_view_measurable_cost_micros,
    CAST(cost_per_conversion AS double precision) AS cost_per_conversion,
    CAST(active_view_measurability AS double precision) AS active_view_measurability,
    CAST(base_ad_group AS character varying) AS base_ad_group,
    CAST(cost_micros AS bigint) AS cost_micros,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced
FROM ad_group_stats