{%- call statement('safety_event', fetch_result=True) -%}
    SELECT column_name
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'public'
      AND table_name = 'safety_score_weights'
      AND column_name NOT IN ('year', 'id', 'month');
{%- endcall -%}

{%- set sorted_safety_events = load_result('safety_event')['data'] | sort -%}
{%- set sorted_motive_events = get_motive_event_type() | sort -%}

{%- set matched_event = [] -%}
{%- for motive_event in sorted_motive_events -%}
    {%- for safety_event in sorted_safety_events -%}
        {%- set safety_event_str = safety_event|string 
            | replace("(", "") 
            | replace(")", "") 
            | replace(",", "") 
            | replace("'", "") 
        -%}
        {%- if motive_event in safety_event_str -%}
            {%- set _ignore = matched_event.append(motive_event) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}
{%- set distinct_events = matched_event -%}

{# Build a list of motive events NOT in the distinct events #}
{%- set missing_from_events = [] -%}
{%- for item in sorted_motive_events -%}
    {%- if item not in distinct_events -%}
        {%- set _ignore = missing_from_events.append(item) -%}
    {%- endif -%}
{%- endfor -%}

{% set reporting_year = get_reporting_year() %} 

WITH safety_score_weights AS (
  SELECT * 
  FROM {{ ref('stg_motive_safety_scores')}}
)

SELECT 
    * 
    {%- for missing_event in missing_from_events %}
    , 0 AS points_{{missing_event}} 
    {%- endfor %}
FROM safety_score_weights
WHERE year = {{ reporting_year}}