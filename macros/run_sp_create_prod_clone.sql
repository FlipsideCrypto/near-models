{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call near._internal.create_prod_clone(
        'near',
        'near_dev',
        'dbt_cloud_near'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
