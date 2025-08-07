{% macro get_previous_month_str(month, event_type) %}
    {% set this_month = (modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d") %}
    {% set date_obj = modules.datetime.datetime.strptime(this_month, "%Y-%m-%d").date() %}
    {% set month_str_list = var("months_list") %}
    {{ return( month_str_list[date_obj.month - 2] )}}

{% endmacro %}