{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% if target.database != "NEAR_COMMUNITY_DEV" %}
            {% set sql %}
                CREATE schema if NOT EXISTS silver;
                CREATE schema if NOT EXISTS streamline;
                {{ create_udf_introspect() }}
                {{ create_udf_s3_list_directories() }}
                {{ create_udf_s3_list_objects() }}
                {{ create_udf_s3_copy_objects() }}
                {{ create_udf_s3_copy_objects_overwrite() }}
                {{ create_UDTF_CALL_CONTRACT_FUNCTION() }}
                {{ create_UDTF_CALL_CONTRACT_FUNCTION_BY_HEIGHT() }}
                {{ create_UDTF_CALL_CONTRACT_FUNCTION_BY_ID() }}
            {% endset %}
            {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}
        {% endif %}
    {% endif %}
{% endmacro %}
