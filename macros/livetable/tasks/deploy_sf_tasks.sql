{% macro deploy_sf_tasks() %}
    {{ create_fact_tx_sproc_task() }}
{% endmacro %}
