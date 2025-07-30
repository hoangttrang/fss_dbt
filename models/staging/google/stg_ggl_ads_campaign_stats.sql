WITH campaign_stats AS (
    SELECT * FROM {{ source('google_ads', 'campaign_stats') }}
)

SELECT 
    CAST(customer_id AS bigint) AS customer_id,
    CAST(date AS date) AS date,
    CAST(_fivetran_id AS character varying) AS _fivetran_id,
    CAST(base_campaign AS character varying) AS base_campaign,
    CAST(conversions_value AS double precision) AS conversions_value,
    CAST(conversions AS double precision) AS conversions,
    CAST(interactions AS bigint) AS interactions,
    CAST(ad_network_type AS character varying) AS ad_network_type,
    CAST(interaction_event_types AS character varying) AS interaction_event_types,
    CAST(id AS bigint) AS id,
    CAST(impressions AS bigint) AS impressions,
    CAST(active_view_viewability AS double precision) AS active_view_viewability,
    CAST(device AS character varying) AS device,
    CAST(view_through_conversions AS bigint) AS view_through_conversions,
    CAST(active_view_impressions AS bigint) AS active_view_impressions,
    CAST(clicks AS bigint) AS clicks,
    CAST(active_view_measurable_impressions AS bigint) AS active_view_measurable_impressions,
    CAST(active_view_measurable_cost_micros AS bigint) AS active_view_measurable_cost_micros,
    CAST(active_view_measurability AS double precision) AS active_view_measurability,
    CAST(cost_micros AS bigint) AS cost_micros,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced
FROM campaign_stats