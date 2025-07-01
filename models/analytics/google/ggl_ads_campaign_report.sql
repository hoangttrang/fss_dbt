{{ config(enabled=var('ad_reporting__google_ads_enabled', True)) }}

WITH stats AS (

    SELECT *
    FROM {{ ref('int_ggl_campaign_stats') }}
), 

accounts AS (

    SELECT *
    FROM {{ ref('int_ggl_account_history') }}
    WHERE is_most_recent_record = True
), 

campaigns AS (

    SELECT *
    FROM {{ ref('int_ggl_campaign_history') }}
    WHERE is_most_recent_record = True
), 

fields AS (

    SELECT
        stats.source_relation,
        stats.date_day,
        accounts.account_name,
        accounts.account_id,
        accounts.currency_code,
        campaigns.campaign_name,
        stats.campaign_id,
        campaigns.advertising_channel_type,
        campaigns.advertising_channel_subtype,
        campaigns.status,
        SUM(stats.spend) AS spend,
        SUM(stats.clicks) AS clicks,
        SUM(stats.impressions) AS impressions,
        SUM(conversions) AS conversions,
        SUM(conversions_value) AS conversions_value,
        SUM(view_through_conversions) AS view_through_conversions

        {{ google_ads_persist_pass_through_columns(pass_through_variable='google_ads__campaign_stats_passthrough_metrics', identifier='stats', transform='sum', coalesce_with=0, exclude_fields=['conversions','conversions_value','view_through_conversions']) }}

    FROM stats
    LEFT JOIN campaigns
        ON stats.campaign_id = campaigns.campaign_id
        AND stats.source_relation = campaigns.source_relation
    LEFT JOIN accounts
        ON campaigns.account_id = accounts.account_id
        AND campaigns.source_relation = accounts.source_relation
    {{ dbt_utils.group_by(10) }}
)

SELECT *
FROM fields