
WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
) 

, data_inspections AS (
    SELECT * FROM {{ ref('stg_motive_data_inspections') }}
)

, driving_periods AS (
	SELECT b.group_name
	  ,b.translated_site
	  ,a.vehicle_id
	  ,cast(start_date AS date) AS date
	  ,COUNT(DISTINCT event_id) AS trips
	FROM public.data_driving_periods a
	JOIN vehicle_map_rs b
	ON a.vehicle_id = b.vehicle_id
	WHERE 1=1
	--AND driving_distance > 0.5
	AND start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	GROUP BY b.group_name, b.translated_site, a.vehicle_id, cast(start_date AS date)
)

, inspections AS (
	SELECT 
		  b.translated_site
		  ,b.group_name
		  ,date
		  ,COUNT(DISTINCT inspection_id) AS inspections
	FROM data_inspections a
	JOIN vehicle_map_rs b
	ON a.vehicle_id = b.vehicle_id
	WHERE 1=1
	AND date  BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	GROUP BY b.translated_site, b.group_name, a.vehicle_id, date
)

, for_aggregation AS (
	SELECT 
		dp.group_name
		,dp.translated_site
		,dp.vehicle_id
		,dp.date
		,dp.trips
		,insp.inspections
		,CASE WHEN dp.trips >= 1 AND insp.inspections >= 1 THEN 1 ELSE 0 END AS dvir_completed
		--,case when dp.trips = insp.inspections then 1 else 0 end AS dvir_completed
	FROM driving_periods dp
	LEFT JOIN inspections insp
		ON (TRIM(BOTH FROM dp.group_name) = TRIM(BOTH FROM insp.group_name)) AND dp.date = insp.date
)


, dvir_completion_cal AS (
	SELECT 
		translated_site,
		TO_CHAR(TO_DATE('2023-' || EXTRACT(MONTH FROM date) || '-01', 'YYYY-MM-DD'), 'FMMonth') AS mnth,
		AVG(dvir_completed) AS dvir_completion
	FROM for_aggregation AS fa
	GROUP BY TO_CHAR(TO_DATE('2023-' || EXTRACT(MONTH FROM date) || '-01', 'YYYY-MM-DD'), 'FMMonth'), translated_site
)

, monthly_dvir_completion AS ( 
	SELECT 
    translated_site AS "Location",
	'DVIR Completion' AS "Metric"
	{%- for month in var('months_list') -%}
    , AVG(CASE WHEN mnth = '{{ month }}' THEN dvir_completion END) AS "{{ month }}"
    {%- endfor %}
	FROM dvir_completion_cal
	GROUP BY translated_site
	ORDER BY translated_site
) 

, base_grid AS (
    SELECT
		DISTINCT
        vm.region AS "Region",
        vm.translated_site AS "Location",
        'DVIR Completion' AS "Metric"
    FROM vehicle_map_rs vm
	WHERE translated_site is NOT NULL
)



, dvir_all_sites AS (
    SELECT 
        bg."Region",
        bg."Location",
        bg."Metric"
	{%- for month in var('months_list') -%}
        , COALESCE(dc."{{ month }}", 0) AS "{{ month }}"
    {%- endfor %}
    FROM base_grid bg
    LEFT JOIN monthly_dvir_completion dc
        ON bg."Location" = dc."Location"
        AND bg."Metric" = dc."Metric"
)

SELECT *
FROM dvir_all_sites


