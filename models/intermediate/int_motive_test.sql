{% set safety_score_event = dbt_utils.star(
    from=ref('int_safety_score_weights_year'),
    except=["year", "id", "month"])
%}
{# Remove double quotes, then split on commas #}
{%- set columns_str = safety_score_event | replace('"', '') %}
{%- set safety_event_list_pts = [] %}
{%- set safety_event_list = [] %}

{#Convert safety score columns into a list#}
{%- for col in columns_str.split(',') -%}
    {%- set _ = safety_event_list_pts.append(col.strip()) -%}
{%- endfor -%}

{# Create a list of events#}
{%- for col in columns_str.split(',') -%}
    {%- set _ = safety_event_list.append(col.strip() | replace('points_', '')) -%}
{%- endfor -%}
{{safety_event_list}}
{{safety_event_list_pts}}