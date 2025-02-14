-- TODO slated for deprecation and drop

{# Deprecated 9/25/2023 #}

{% macro get_nearblocks_fts() %}
{% set sql %}

CALL {{ target.database }}.bronze_api.get_nearblocks_fts()

{% endset %}
{% do run_query(sql) %}
{% endmacro%}
