{# this does nothing until the other macro is used to actually create the procedure
but idt ls is possible in a procedure so maybe just delete this side of it
 #}
{% macro get_s3_filenames() %}
{% set sql %}

CALL {{ target.database }}.bronze_api.get_s3_filenames()

{% endset %}
{% do run_query(sql) %}
{% endmacro%}
