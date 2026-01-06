WITH driving_w_vehicle_map AS (
    SELECT * FROM {{ ref('int_motive_data_driving_period_vehicle_map') }}
)


SELECT 
    *
FROM driving_w_vehicle_map a
WHERE 1=1
AND a.start_date BETWEEN  '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'