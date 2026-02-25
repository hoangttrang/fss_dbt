{% macro get_motive_negative_event_type() %}
{% set event_types =
        dbt_utils.get_column_values(
            table=ref('stg_motive_data_combined_events'),
            column='type'
        )
    %}
{{ return(event_types) }}
{% endmacro %}
