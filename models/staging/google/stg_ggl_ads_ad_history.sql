WITH ad_history AS (
    SELECT * FROM {{ source('google_ads', 'ad_history') }}
)

SELECT 
    CAST(ad_group_id AS bigint) AS ad_group_id,
    CAST(id AS bigint) AS id,
    CAST(updated_at AS timestamp with time zone) AS updated_at,
    CAST(action_items AS character varying) AS action_items,
    CAST(ad_strength AS character varying) AS ad_strength,
    CAST(added_by_google_ads AS boolean) AS added_by_google_ads,
    CAST(device_preference AS character varying) AS device_preference,
    CAST(display_url AS character varying) AS display_url,
    CAST(final_url_suffix AS character varying) AS final_url_suffix,
    CAST(final_app_urls AS character varying) AS final_app_urls,
    CAST(final_mobile_urls AS character varying) AS final_mobile_urls,
    CAST(final_urls AS character varying) AS final_urls,
    CAST(name AS character varying) AS name,
    CAST(policy_summary_approval_status AS character varying) AS policy_summary_approval_status,
    CAST(policy_summary_review_status AS character varying) AS policy_summary_review_status,
    CAST(status AS character varying) AS status,
    CAST(system_managed_resource_source AS character varying) AS system_managed_resource_source,
    CAST(tracking_url_template AS character varying) AS tracking_url_template,
    CAST(type AS character varying) AS type,
    CAST(url_collections AS character varying) AS url_collections,
    CAST(_fivetran_synced AS timestamp with time zone) AS _fivetran_synced,
    CAST(_fivetran_start AS timestamp with time zone) AS _fivetran_start,
    CAST(_fivetran_end AS timestamp with time zone) AS _fivetran_end,
    CAST(_fivetran_active AS boolean) AS _fivetran_active
FROM ad_history