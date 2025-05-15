{% macro deploy_sf_tasks() %}
    {{ create_fact_tx_sproc_task() }}
    {{ create_bronze_tx_sproc_task() }}
{% endmacro %}
