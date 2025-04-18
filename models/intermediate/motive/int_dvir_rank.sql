{% set month_str = get_current_month_str() %}

with vehicle_map_rs AS (
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
select dp.group_name
      ,dp.translated_site
	  ,dp.vehicle_id
	  ,dp.date
	  ,dp.trips
	  ,insp.inspections
	  ,case when dp.trips >= 1 and insp.inspections >= 1 then 1 else 0 end as dvir_completed
	  --,case when dp.trips = insp.inspections then 1 else 0 end as dvir_completed
from driving_periods dp
left join inspections insp
on dp.group_name = insp.group_name
and dp.date = insp.date
)

,  ranked_trips_for_inspections AS (
    SELECT 
        a.id
		,a.event_id
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
		,b.id
		,b.number
		,b.status
		,b.make
		,b.model
		,b.group_name
		,b.region
        ,b.translated_site
        ,ROW_NUMBER() OVER (
            PARTITION BY a.vehicle_id, a.start_date
            ORDER BY CASE WHEN driver_id IS NOT NULL THEN 1 ELSE 2 END
        ) AS row_num
    FROM public.data_driving_periods a
	join vehicle_map_rs b	
	on a.vehicle_id = b.vehicle_id
	WHERE 1=1
	and driving_distance > 0.5
	and start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
),

trips_for_inspections as
(
select translated_site, region, cast(start_date as date) as date, coalesce(driver_id, -1) as driver_id, driver_company_id, vehicle_id, number, count(event_id) as n_trips
from ranked_trips_for_inspections a
where 1=1
and row_num = 1
group by translated_site, region, cast(start_date as date), coalesce(driver_id, -1), driver_company_id, vehicle_id, number
),

compliance_data as (
select a.translated_site, a.region, a.date, a.driver_id, a.driver_company_id, a.vehicle_id, a.number, a.n_trips, coalesce(b.n_inspections, 0) as n_inspections,
case when n_inspections > 0 then 1 else 0 end as compliant
from trips_for_inspections a
left join (
	select date, driver_id, vehicle_id, count(id) as n_inspections
	from data_inspections
	where 1=1
	and inspection_type = 'pre_trip'
	group by date, driver_id, vehicle_id
	) b
on a.date = b.date
and a.driver_id = b.driver_id
and a.vehicle_id = b.vehicle_id
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


, dvir_completion AS (
    select translated_site AS "Location", region AS "Region"
    , 'DVIR Completion' AS "Metric", avg(compliant) as "{{month_str}}"
from compliance_data
group by translated_site, region
)


, dvir_all_sites AS (
    SELECT 
        bg."Region",
        bg."Location",
        bg."Metric"
        , COALESCE(dc."{{ month_str }}", 0) AS "{{ month_str }}"
    FROM base_grid bg
    LEFT JOIN dvir_completion dc
        ON bg."Region" = dc."Region"
        AND bg."Location" = dc."Location"
        AND bg."Metric" = dc."Metric"
)

SELECT *,
    RANK() OVER (PARTITION BY "Metric" ORDER BY "{{ month_str }}" DESC) AS "Company Rank",
    RANK() OVER (PARTITION BY "Metric", "Region" ORDER BY "{{ month_str }}" DESC) AS "Region Rank"
FROM dvir_all_sites
WHERE "Location" IS NOT NULL
ORDER BY "Metric", "Region", "Location"


