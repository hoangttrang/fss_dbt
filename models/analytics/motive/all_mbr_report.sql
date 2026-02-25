{%- set months = [
  ('January','01'),
  ('February','02'),
  ('March','03'),
  ('April','04'),
  ('May','05'),
  ('June','06'),
  ('July','07'),
  ('August','08'),
  ('September','09'),
  ('October','10'),
  ('November','11'),
  ('December','12')
] -%}

{# Put the metric names you want as output columns here #}
{%- set distinct_metrics = dbt_utils.get_column_values(table=ref('mbr_report'), column = '"Metric"') -%}

with long as (

  {#  2024 #}
  {%- for m, mm in months %}
    {% if not loop.first %} union all {% endif %}
    select
        "Region"   
      , "Location"
      , "Metric"  
      , cast('2024-{{ mm }}-01' as date) as year_month
      , '2024-{{ mm }}' as year_month_in_string
      , round({{ adapter.quote(m) }}::numeric, 3) as metric_value
    from dbt_analytics.mbr_report_2024
  {%- endfor %}

  union all

  {#  2025  #}
  {%- for m, mm in months %}
    {% if not loop.first %} union all {% endif %}
    select
        "Region"   
      , "Location" 
      , "Metric"   
      , cast('2025-{{ mm }}-01' as date) as year_month
      , '2025-{{ mm }}' as year_month_in_string
      , round({{ adapter.quote(m) }}::numeric, 3) as metric_value
    from dbt_analytics.mbr_report_2025
  {%- endfor %}
  union all
    {#  2026  #}
  {%- for m, mm in months %}
    {% if not loop.first %} union all {% endif %}
    select
        "Region"   
      , "Location" 
      , "Metric"   
      , cast('2026-{{ mm }}-01' as date) as year_month
      , '2026-{{ mm }}' as year_month_in_string
      , round({{ adapter.quote(m) }}::numeric, 3) as metric_value
    from {{ ref('mbr_report') }}
  {%- endfor %}

)
select * from long
-- select
--   region,
--   location,
--   year_month,
--   year_month_in_string
--   {%- for met in distinct_metrics %}
--   , max(case when metric = '{{ met }}' then metric_value end)
--       as {{ met | lower | replace(' ', '_') | replace('-', '_') }}
--   {%- endfor %}
-- from long
-- group by 1,2,3,4