
WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
) 

, data_inspections AS (
    SELECT * FROM public.data_inspections
)

, driving_periods as (
	select b.group_name
	  ,b.translated_site
	  ,a.vehicle_id
	  ,cast(start_date as date) as date
	  ,COUNT(DISTINCT event_id) as trips
	from public.data_driving_periods a
	join vehicle_map_rs b
	on a.vehicle_id = b.vehicle_id
	where 1=1
	--and driving_distance > 0.5
	and start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	group by b.group_name, b.translated_site, a.vehicle_id, cast(start_date as date)
)

, inspections as (
	select 
		  b.translated_site
		  ,b.group_name
		  ,date
		  ,COUNT(DISTINCT inspection_id) as inspections
	from data_inspections a
	join vehicle_map_rs b
	on a.vehicle_id = b.vehicle_id
	where 1=1
	and date  BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
	group by b.translated_site, b.group_name, a.vehicle_id, date
),

for_aggregation as (
select 
	   dp.group_name
      ,dp.translated_site
	  ,dp.vehicle_id
	  ,dp.date
	  ,dp.trips
	  ,insp.inspections
	  ,case when dp.trips >= 1 and insp.inspections >= 1 then 1 else 0 end as dvir_completed
	  --,case when dp.trips = insp.inspections then 1 else 0 end as dvir_completed
from driving_periods dp
left join inspections insp
	on dp.group_name = insp.group_name and dp.date = insp.date
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


