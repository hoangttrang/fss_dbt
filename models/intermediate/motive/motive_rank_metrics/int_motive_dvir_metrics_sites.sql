
WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
) 

, dvir_calculation AS (
	SELECT * FROM {{ ref('int_motive_dvir_calculation') }}
)


, dvir_completion_cal AS (
	SELECT 
		translated_site,
		TO_CHAR(TO_DATE('2023-' || EXTRACT(MONTH FROM date) || '-01', 'YYYY-MM-DD'), 'FMMonth') AS mnth,
		AVG(dvir_completed) AS dvir_completion
	FROM dvir_calculation AS fa
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


