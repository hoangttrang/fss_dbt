{% macro get_current_month(month, event_type) %}
    {% set this_month = var("mbr_report_date") %}
    {% set date_obj = modules.datetime.datetime.strptime(this_month, "%Y-%m-%d").date() %}
    {% set month_str_list = var("months_list") %}
    {{ return( month_str_list[date_obj.month - 1] )}}

{% endmacro %}