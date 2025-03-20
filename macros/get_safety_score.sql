{% macro get_safety_score(month, event_type) %}
    {% set points = dbt_utils.get_column_values(table=ref('int_safety_score_weights_year'), column=event_type, where ="month = '" ~ month ~ "'" )%}
    {{return(points)}}
{% endmacro %}





                

