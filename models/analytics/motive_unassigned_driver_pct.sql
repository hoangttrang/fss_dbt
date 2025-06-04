WITH unassigned_driver AS (
    SELECT * FROM {{ ref('int_motive_unassigned_driver') }}
)

SELECT 
    CAST(start_date AS DATE) AS start_date,
    site,
    region,
    group_name,
    SUM(CASE WHEN unassigned = 1 THEN 1 ELSE 0 END) AS unassigned_count,
    SUM(CASE WHEN unassigned = 0 THEN 1 ELSE 0 END) AS assigned_count,
    COUNT(*) AS total_count
FROM unassigned_driver
GROUP BY 
    CAST(start_date AS DATE),
    site,
    region,
    group_name
