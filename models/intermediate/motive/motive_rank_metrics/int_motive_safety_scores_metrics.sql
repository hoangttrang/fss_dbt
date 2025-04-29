-- depends_on: {{ ref('main_safety_score_config') }}
{%- call statement('safety_score', fetch_result=True) %}
    SELECT motive_event_name, value 
    FROM {{ ref('main_safety_score_config') }}
    WHERE sub_event2 IS NOT NULL AND value IS NOT NULL
{%- endcall -%}

-- get safety score from main_safety_score_config table
{%- set safety_score_results = load_result('safety_score')['data'] %}
{%- set safety_score_map = {} %}
-- create a dictionary of safety scores and corresponding event types
{%- for row in safety_score_results %}
    {%- set _ = safety_score_map.update({ row[0]: row[1] }) %}
{%- endfor %}

{% set month_str = get_current_month_str() %}

-- Get sliced months from Jan to current months: 

{%- set current_month = get_current_month_str() %}
{%- set month_index = var('months_list').index(current_month) %}
{%- set valid_months = var('months_list')[:month_index + 1] %}
{%- set remaining_months = var('months_list')[month_index + 1:] %} 

WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, event_breakdown AS (
    SELECT * FROM {{ ref('int_motive_event_breakdown') }}
)


{% set safety_score_event = dbt_utils.star(
    from=ref('int_safety_score_weights_year'),
    except=["year", "id", "month"])
%}
{# Remove double quotes, then split on commas #}
{%- set columns_str = safety_score_event | replace('"', '') %}
{%- set safety_event_list = [] %}

{#Convert safety score columns into a list#}
{%- for col in columns_str.split(',') -%}
    {%- set _ = safety_event_list.append(col.strip()) -%}
{%- endfor -%}


, safety_score_weights_by_mnth AS (SELECT 
    region 
    , translated_site
    {%- for month in var('months_list') %}
        {%set month_str =  month[:3]%}
        {%- for event_type in safety_event_list %}
            {%- set score_value = get_safety_score(month_str,event_type) %}
            , {{ score_value[0] }}  AS {{ month | lower }}_{{ event_type }}
        {%- endfor %}
    {%- endfor %}
FROM vehicle_map_rs
WHERE translated_site IS NOT NULL
)

, safety_score_weights AS (
    SELECT DISTINCT *
    FROM safety_score_weights_by_mnth
)

, rank_trips AS (
    SELECT * FROM {{ ref('int_motive_rank_trips') }}
)

, drive_distances AS (SELECT 
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site
    {% for month in var('months_list') %}
    , SUM(CASE WHEN LOWER(rank_trips.month) = LOWER('{{month}}') THEN rank_trips.driving_distance ELSE 0 END) AS {{month | lower}}_miles_driven
    {% endfor %}
FROM rank_trips 
JOIN vehicle_map_rs
    ON rank_trips.vehicle_id = vehicle_map_rs.vehicle_id
WHERE rank_trips.start_date BETWEEN '{{var("mbr_start_date")}}' AND '{{ var("mbr_report_date")}}'
    AND rank_trips.driving_distance > 0
    AND rank_trips.driving_distance <= 500
    AND rank_trips.row_num = 1
GROUP BY 
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site
)

, combined_dd_and_eb AS ( 
    SELECT 
    {%- for month in var('months_list') %}
        dd.{{month | lower}}_miles_driven,
    {%- endfor %}
	eb.*
	FROM  drive_distances dd
	LEFT JOIN event_breakdown eb
	ON eb.region = dd.region AND eb.translated_site = dd.translated_site
)

, safety_score_breakdown AS (
    SELECT 
    cddeb.region, 
    cddeb.translated_site, 
    {%- for month in var('months_list') %}
       {{month | lower}}_miles_driven,
    {%- endfor %}

    -- Calculate total safety points for 12 months: 
    {%- for month in var('months_list') %}
        (
        {%- for event_type in var('motive_event_type') %}
            ssw.{{ month | lower}}_points_{{event_type}} * {{month | lower}}_{{event_type}}
            {%- if not loop.last %} + {% endif %}
        {%- endfor -%}
        ) AS {{ month | lower }}_total_points_for_score
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}
    FROM combined_dd_and_eb cddeb
    LEFT JOIN safety_score_weights ssw 
      ON cddeb.translated_site = ssw.translated_site
)

, safety_scores_unranked AS (
    SELECT 
    ssb.region as "Region"
    , ssb.translated_site as "Location"
    , 'Site Safety Score' as "Metric"
    -- Calculate the safety score for each month over 100:
    {%- for month in valid_months %}
        , 100 - COALESCE(ssb.{{month | lower}}_total_points_for_score / NULLIF(ssb.{{month | lower}}_miles_driven, 0), 0) * 1000 AS "{{month}}"
    {%- endfor %}

    -- Only if remaining_months is not empty, add 0 as placeholder
    {%- if remaining_months | length > 0 %}
        {%- for month in remaining_months %}
            , 0 AS "{{month}}"
        {%- endfor %}
    {%- endif %}

    FROM safety_score_breakdown ssb
    WHERE ssb.region IS NOT NULL
)

SELECT *
FROM safety_scores_unranked
 

