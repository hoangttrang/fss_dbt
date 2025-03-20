SELECT 
    r.region,
    s.translated_site,
    SUM(CASE WHEN a.month = 'January' THEN a.driving_distance ELSE 0 END) AS january_miles_driven,
    SUM(CASE WHEN a.month = 'February' THEN a.driving_distance ELSE 0 END) AS february_miles_driven,
    SUM(CASE WHEN a.month = 'March' THEN a.driving_distance ELSE 0 END) AS march_miles_driven,
    SUM(CASE WHEN a.month = 'April' THEN a.driving_distance ELSE 0 END) AS april_miles_driven,
    SUM(CASE WHEN a.month = 'May' THEN a.driving_distance ELSE 0 END) AS may_miles_driven,
    SUM(CASE WHEN a.month = 'June' THEN a.driving_distance ELSE 0 END) AS june_miles_driven,
    SUM(CASE WHEN a.month = 'July' THEN a.driving_distance ELSE 0 END) AS july_miles_driven,
    SUM(CASE WHEN a.month = 'August' THEN a.driving_distance ELSE 0 END) AS august_miles_driven,
    SUM(CASE WHEN a.month = 'September' THEN a.driving_distance ELSE 0 END) AS september_miles_driven,
    SUM(CASE WHEN a.month = 'October' THEN a.driving_distance ELSE 0 END) AS october_miles_driven,
    SUM(CASE WHEN a.month = 'November' THEN a.driving_distance ELSE 0 END) AS november_miles_driven,
    SUM(CASE WHEN a.month = 'December' THEN a.driving_distance ELSE 0 END) AS december_miles_driven
FROM {{ ref('int_motive_rank_trips') }} a
JOIN {{ ref('stg_motive_data_vehicle_group_mappings') }} b
    ON a.vehicle_id = b.vehicle_id
LEFT JOIN {{ ref('site_translation') }} s 
    ON b.group_name = s.site
LEFT JOIN {{ ref('region_translation') }} r 
    ON b.group_name = r.site
WHERE a.start_date BETWEEN '2025-01-01' AND '2025-02-01'
  AND a.driving_distance > 0
  AND a.driving_distance <= 500
  AND a.row_num = 1
GROUP BY r.region, s.translated_site