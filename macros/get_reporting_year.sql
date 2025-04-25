{% macro get_reporting_year(month, event_type) %}
    {% set reporting_date = var("mbr_report_date") %}
    {% set date_obj = modules.datetime.datetime.strptime(reporting_date, "%Y-%m-%d").date() %}
    {{ return( date_obj.year  )}}

{% endmacro %}