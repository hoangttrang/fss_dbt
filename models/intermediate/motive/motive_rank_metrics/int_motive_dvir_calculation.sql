
WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
) 

, data_inspections AS (
    SELECT * FROM {{ ref('stg_motive_data_inspections') }}
)

, data_driving_periods AS (
    SELECT * FROM {{ ref('stg_motive_data_driving_periods') }}
)

, driving_periods AS (
	SELECT 
        b.group_name
        , b.region
        , b.translated_site
        , a.vehicle_id
        , cast(start_date AS date) AS date
        , COUNT(DISTINCT event_id) AS trips
	FROM data_driving_periods a
	JOIN vehicle_map_rs b
	ON a.vehicle_id = b.vehicle_id
	WHERE 1=1
	--AND driving_distance > 0.5
	AND start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	GROUP BY 
        b.region,
        b.group_name, 
        b.translated_site, 
        a.vehicle_id, 
        cast(start_date AS date)
)

, inspections AS (
	SELECT 
        b.region
		, b.translated_site
		, b.group_name
		, date
		, COUNT(DISTINCT inspection_id) AS inspections
	FROM data_inspections a
	JOIN vehicle_map_rs b
	ON a.vehicle_id = b.vehicle_id
	WHERE 1=1
	AND date  BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	GROUP BY 
        b.region,
        b.translated_site, 
        b.group_name, 
        a.vehicle_id, date
)

, for_aggregation AS (
	SELECT 
		dp.group_name
        , dp.region
		, dp.translated_site
		, dp.vehicle_id
		, dp.date
		, dp.trips
		, insp.inspections
		, CASE WHEN dp.trips >= 1 AND insp.inspections >= 1 THEN 1 ELSE 0 END AS dvir_completed
		--,case when dp.trips = insp.inspections then 1 else 0 end AS dvir_completed
	FROM driving_periods dp
	LEFT JOIN inspections insp
		ON (TRIM(BOTH FROM dp.group_name) = TRIM(BOTH FROM insp.group_name)) AND dp.date = insp.date
)


SELECT * FROM for_aggregation
WHERE translated_site IS NOT NULL