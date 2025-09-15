WITH campaign_stats AS (
    SELECT *
    FROM {{ ref('stg_ggl_ads_campaign_stats') }}
)

, search_term_stats AS (
    SELECT *
    FROM {{ ref('stg_ggl_ads_search_term_stats') }}
)

, campaign_report AS (
    SELECT *
    FROM {{ ref ('ggl_ads_campaign_report') }}
)

, conversions_by_campaign AS (
    SELECT 
        date,
        id AS campaign_id,
        SUM(conversions) AS num_conversions
    FROM campaign_stats
    GROUP BY date, id
)

, search_term_summary AS (
    SELECT 
        date,
        campaign_id,
        ROUND(SUM(impressions)) AS n_impressions,
        ROUND(SUM(impressions * top_impression_percentage)) AS top_impressions_count
    FROM search_term_stats
    GROUP BY date, campaign_id
)

SELECT
    cr.account_id,
	cr.account_name,
	cr.campaign_id,
	cr.campaign_name,
	cr.date_day,
	EXTRACT(YEAR FROM cr.date_day) AS date_year,
	EXTRACT(MONTH FROM cr.date_day) AS date_month,
	cr.spend,
	cr.clicks,
    cb.num_conversions,
    sts.n_impressions,
    sts.top_impressions_count
FROM 
    campaign_report cr
LEFT JOIN
    conversions_by_campaign cb
    ON cr.date_day = cb.date AND cr.campaign_id = cb.campaign_id
LEFT JOIN
    search_term_summary sts
    ON cr.date_day = sts.date AND cr.campaign_id = sts.campaign_id
ORDER BY
    cr.date_day DESC, cr.campaign_id