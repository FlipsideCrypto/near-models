{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call flow._internal.create_prod_clone(
        'near',
        'near_dev',
        'public'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
