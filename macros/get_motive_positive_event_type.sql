{% macro get_motive_positive_event_type() %}

{% set positive_behaviors =
        dbt_utils.get_column_values(
            table=ref('stg_motive_data_positive_behavior_events'),
            column='positive_behavior'
        )
%}
{{ return(positive_behaviors) }}
{% endmacro %}
