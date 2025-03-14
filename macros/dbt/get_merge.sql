{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% if incremental_predicates[0] == "dynamic_range_predicate_custom" %}
        {% set predicates = [dynamic_range_predicate_custom(source, incremental_predicates[1], "DBT_INTERNAL_DEST")] %}
        {% set merge_sql = fsc_utils.get_merge_sql(target, source, unique_key, dest_columns, predicates) %}
    {% else %}
        {% set merge_sql = fsc_utils.get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {% endif %}
    {{ return(merge_sql) }}
{% endmacro %}
