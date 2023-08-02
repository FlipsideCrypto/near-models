{% macro create_udf_decode_bytelist_2() %}
{% set sql %}
{# 
    A UDF to decode a list of bytes into a string.
    Note this is considerably slower than a SQL alternative.
 #}
CREATE OR REPLACE FUNCTION
    {{ target.database }}.silver.UDF_DECODE_BYTELIST_2(
        bytelist ARRAY
    )
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'decode_bytelist'
AS
$$
def decode_bytelist(bytelist):
    return bytes(bytelist).decode('utf-8')
$$
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
