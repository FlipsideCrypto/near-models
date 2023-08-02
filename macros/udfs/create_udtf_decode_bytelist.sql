{% macro create_udtf_decode_bytelist() %}
{% set sql %}
{# 
    A UDTF decoding a list of bytes into a hex-encoded string and a JSON object.
 #}
CREATE OR REPLACE FUNCTION
    {{ target.database }}.silver.UDTF_DECODE_BYTELIST(
        bytelist ARRAY
    )
RETURNS TABLE
    (
        input ARRAY,
        hex_encoded_response STRING,
        decoded_response VARIANT
    )
AS
$$
WITH
input as (
    select
        bytelist
),
try_decode_hex AS (
    SELECT
        bytelist,
        b.value AS raw,
        b.index,
        LPAD(TRIM(to_char(b.value :: INT, 'XXXXXXX')) :: STRING, 2, '0') AS hex
    FROM
        input A,
        TABLE(FLATTEN(bytelist, recursive => TRUE)) b
),
concat_res AS (
    SELECT
        bytelist,
        ARRAY_TO_STRING(ARRAY_AGG(hex) within GROUP (
    ORDER BY
        INDEX ASC), '') AS hex_encoded_response
    FROM
        try_decode_hex
)
SELECT
    bytelist as input,
    hex_encoded_response,
    TRY_PARSE_JSON(livequery.utils.udf_hex_to_string(hex_encoded_response)) AS decoded_response
FROM
    concat_res
$$
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
