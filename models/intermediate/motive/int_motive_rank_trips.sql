SELECT 
    a.event_id
    ,a.driver_id
    ,a.driver_first_name
    ,a.driver_last_name
    ,a.vehicle_id
    ,a.start_date
    ,a.end_date
    ,a.driving_distance
    ,a.driving_period_type
    ,a.driver_company_id
    ,a.minutes_driving
    ,a.month
    ,a.created_at
    ,a.updated_at
    ,a.unassigned
    ,b.number
    ,b.status
    ,b.make
    ,b.model
    ,b.group_name
    ,s.translated_site
    ,r.region
    ,ROW_NUMBER() OVER (
        PARTITION BY a.vehicle_id, a.start_date
        ORDER BY CASE WHEN driver_id IS NOT NULL THEN 1 ELSE 2 END
    ) AS row_num
FROM  {{ ref('stg_motive_data_driving_periods') }} a
JOIN {{ ref('stg_motive_data_vehicle_group_mappings') }} b
    on a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref('int_site_translation') }} s 
    ON b.group_name = s.site
LEFT JOIN {{ ref('int_region_translation') }} r
    ON b.group_name = r.site
WHERE 1=1
AND START_DATE BETWEEN '2025-01-01' AND '2025-02-01'
