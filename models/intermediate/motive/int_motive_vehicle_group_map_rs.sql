-- This model maps data vehicle group mappings to site and region translations.

WITH vehicle_map AS (
    SELECT * FROM {{ ref('stg_motive_data_vehicle_group_mappings') }}
)

, site_region_translation AS ( 
    SELECT *  FROM {{ ref('stg_motive_consolidated_sites') }}
)

, distinct_consolidated_sites AS (
    SELECT DISTINCT consolidated_site, region, motive_group_name
    FROM site_region_translation
)

, joined_table AS (SELECT 
    vehicle_map.*
    , distinct_consolidated_sites.consolidated_site AS translated_site
    , distinct_consolidated_sites.region
FROM vehicle_map
LEFT JOIN distinct_consolidated_sites 
    ON (TRIM(BOTH FROM vehicle_map.group_name) = TRIM(BOTH FROM distinct_consolidated_sites.motive_group_name)) ) 

SELECT 
 * 
FROM joined_table
