{% macro nft_metadata() %}

{% set sql %}
    CALL {{ target.database }}.bronze_api.get_near_metadata() 
{% endset %}

{% do run_query(sql) %}
{% endmacro %}