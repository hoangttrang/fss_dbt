
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

WITH vehicle_map_rs AS (
    SELECT * FROM {{ ref('int_motive_vehicle_group_map_rs') }}
)

SELECT 
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