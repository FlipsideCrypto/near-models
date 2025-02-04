{% macro create_udf_extract_hash_array() %}
{% set sql %}
create or replace function {{ target.database}}.STREAMLINE.UDF_EXTRACT_HASH_ARRAY(object_array variant, object_key string) 
returns ARRAY
language python
runtime_version = '3.9'
handler = 'extract_hash_array'
AS
$$
def extract_hash_array(object_array, object_key):
    try:
        return [object[object_key] for object in object_array]
    except:
        return []
$$
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
