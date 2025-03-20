
{% macro get_motive_event_type() %}
    {% set distinct_values = dbt_utils.get_column_values(table=ref('stg_motive_data_combined_events'), column='type') %}
    {{ return(distinct_values) }}
{% endmacro %}

