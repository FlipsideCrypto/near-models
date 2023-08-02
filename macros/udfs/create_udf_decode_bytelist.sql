{% macro create_udf_decode_bytelist() %}
{% set sql %}
{# 
    A UDF to decode a list of bytes into a string.
    Note this is considerably slower than a SQL alternative.
 #}
CREATE OR REPLACE FUNCTION
    {{ target.database }}.silver.UDF_DECODE_BYTELIST(
        bytelist ARRAY
    )
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'decode_bytelist'
AS
$$
def decode_bytelist(bytelist):
    return "".join([chr(i) for i in bytelist]).strip('\"')
$$
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
