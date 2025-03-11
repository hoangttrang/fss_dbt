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

WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

, drive_distances AS (
    SELECT * FROM {{ ref('int_motive_drive_distances') }}
)

, event_breakdown AS (
    SELECT * FROM {{ ref('int_motive_event_breakdown') }}
)

, safety_score_weights AS (
    SELECT DISTINCT
    vehicle_map_rs.region
    , vehicle_map_rs.translated_site
    {%- for month in var('months_list') %} 
        {%- for event_type in var('motive_event_type') %}
            {%- set score_value = safety_score_map.get(event_type, 1) %}
            , {{ score_value }} AS {{ month | lower }}_points_{{ event_type }}
        {%- endfor %}
    {%- endfor %}
    FROM vehicle_map_rs
    WHERE translated_site IS NOT NULL
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
    , 'Safety Score' as "Metric"
    -- Calculate the safety score for each month over 100:
    {%- for month in var('months_list') %}
        , 100 - COALESCE(ssb.{{month | lower}}_total_points_for_score / NULLIF(ssb.{{month | lower}}_miles_driven, 0), 0) * 1000 AS "{{month}}"
    {%- endfor %}
    FROM safety_score_breakdown ssb
    WHERE ssb.region IS NOT NULL
)

SELECT *,
    RANK() OVER (ORDER BY "January" DESC) AS "Company Rank",
    RANK() OVER (PARTITION BY "Region" ORDER BY "January" DESC) AS "Region Rank"
FROM safety_scores_unranked
 

