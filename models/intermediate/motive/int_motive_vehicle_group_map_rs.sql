-- This model maps data vehicle group mappings to site and region translations.

WITH vehicle_map AS (
    SELECT * FROM {{ ref('stg_motive_data_vehicle_group_mappings') }}
)

, site_translation AS (
    SELECT * FROM {{ ref('site_translation') }}
)

, region_translation AS (
    SELECT * FROM {{ ref('region_translation') }}
)

SELECT 
    vehicle_map.*
    , site_translation.translated_site
    , region_translation.region
FROM vehicle_map
LEFT JOIN site_translation 
    ON vehicle_map.group_name = site_translation.site
LEFT JOIN region_translation
    ON vehicle_map.group_name = region_translation.site
