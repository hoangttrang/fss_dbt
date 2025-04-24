-- This model maps data vehicle group mappings to site and region translations.

WITH vehicle_map AS (
    SELECT * FROM {{ ref('stg_motive_data_vehicle_group_mappings') }}
)

, site_region_translation AS ( 
    SELECT * 
    ,   -- Create translated_site with mapping logic
    CASE 
        WHEN consolidated_site IN (
            'NC - ASC - Denver', 
            'NC - ASC - Greensboro', 
            'NC - ASC'
        ) THEN 'NC - ASC'
        
        WHEN consolidated_site IN (
            'SC - PSI Columbia',
            'GA -PSI Augusta'
        ) THEN 'SC - PSI'
        ELSE consolidated_site
    END AS translated_site
    
    FROM {{ ref('stg_motive_consolidated_sites') }}
)

SELECT 
    vehicle_map.*
    , site_region_translation.translated_site
    , site_region_translation.region
FROM vehicle_map
LEFT JOIN site_region_translation 
    ON vehicle_map.group_name = site_region_translation.motive_group_name 
