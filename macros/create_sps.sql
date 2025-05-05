{% macro create_sps() %}
    {% if target.database == 'NEAR' %}
        CREATE schema IF NOT EXISTS _internal;
        {{ sp_create_prod_clone('_internal') }};
    {% endif %}
    {{ create_sp_refresh_fact_transactions_live() }}
{% endmacro %}
