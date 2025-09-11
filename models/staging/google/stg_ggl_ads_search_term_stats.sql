WITH search_term_stats AS (
    SELECT * FROM {{ source('google_ads', 'search_term_stats') }}
)

SELECT 
    CAST(customer_id AS bigint) AS customer_id,
    CAST(date AS date) AS date,
    CAST(_fivetran_id AS character varying(256)) AS _fivetran_id,
    CAST(resource_name AS character varying(256)) AS resource_name,
    CAST(search_term AS character varying(256)) AS search_term,
    CAST(conversions AS double precision) AS conversions,
    CAST(search_term_match_type AS character varying(256)) AS search_term_match_type,
    CAST(campaign_id AS bigint) AS campaign_id,
    CAST(impressions AS bigint) AS impressions,
    CAST(ad_group_id AS bigint) AS ad_group_id,
    CAST(view_through_conversions AS bigint) AS view_through_conversions,
    CAST(absolute_top_impression_percentage AS double precision) AS absolute_top_impression_percentage,
    CAST(clicks AS bigint) AS clicks,
    CAST(status AS character varying(256)) AS status,
    CAST(top_impression_percentage AS double precision) AS top_impression_percentage,
    CAST(conversions_from_interactions_value_per_interaction AS double precision) AS conversions_from_interactions_value_per_interaction,
    CAST(average_cpc AS double precision) AS average_cpc,
    CAST(ctr AS double precision) AS ctr,
    CAST(conversions_from_interactions_rate AS double precision) AS conversions_from_interactions_rate,
    CAST(cost_micros AS bigint) AS cost_micros,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced
FROM search_term_stats