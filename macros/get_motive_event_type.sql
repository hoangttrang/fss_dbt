
{% macro get_motive_event_type() %}
    {% set negative_behaviors = get_motive_negative_event_type() %}
    {% set positive_behaviors = get_motive_positive_event_type() %}
    {% set all_events = negative_behaviors + positive_behaviors %}
    {% set distinct_values = all_events | unique %}
    {{ return(distinct_values) }}
{% endmacro %}

