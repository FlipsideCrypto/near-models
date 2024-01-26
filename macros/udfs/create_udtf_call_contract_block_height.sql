{% macro create_UDTF_CALL_CONTRACT_FUNCTION_BY_HEIGHT() %}
{% set sql %}
{# 
    Execute a method on a contract at a specific block height.
    This function is equivalent to the one defined in macro create_UDTF_CALL_CONTRACT_FUNCTION, except for the block height input parameter.
    Signature STRING, STRING, OBJECT, NUMBER 
 #}
CREATE OR REPLACE FUNCTION
    {{ target.database }}.STREAMLINE.UDTF_CALL_CONTRACT_FUNCTION(
        contract_address STRING,
        method_name STRING,
        args OBJECT,
        block_id NUMBER
    )
RETURNS TABLE (
    BLOCK_HEIGHT NUMBER,
    DATA VARIANT,
    DECODED_RESULT VARIANT,
    ERROR VARIANT
)
AS
$$
WITH params AS (
    SELECT
        lower(contract_address) AS contract_address,
        lower(method_name) AS method,
        block_id,
        BASE64_ENCODE(args::STRING) AS arg_base64
),
call_function AS (
    SELECT
        block_id,
        {{ target.database }}.live.udf_api(
            'POST',
            'https://archival-rpc.mainnet.near.org',
            { 
                'Content-Type': 'application/json' 
            },
            { 
                'jsonrpc': '2.0',
                'id': 'dontcare',
                'method' :'query',
                'params':{
                    'request_type': 'call_function',
                    'block_id': block_id,
                    'account_id': contract_address,
                    'method_name': method_name,
                    'args_base64': arg_base64 }
                    }
        ) AS res
    FROM
        params p
),
response AS (
    SELECT
        block_id,
        res,
        res :data ::VARIANT as data,
        res :data :result :result :: ARRAY AS res_array,
        TRY_PARSE_JSON(
            COALESCE(
                res :data :result :error :: STRING,
                res :data :error :: STRING
            )
        ) AS error
    FROM
        call_function
),
try_decode_hex AS (
    SELECT
        block_id,
        b.value AS raw,
        b.index,
        LPAD(TRIM(to_char(b.value :: INT, 'XXXXXXX')) :: STRING, 2, '0') AS hex
    FROM
        response A,
        TABLE(FLATTEN(res_array, recursive => TRUE)) b
    WHERE
        IS_ARRAY(res_array) = TRUE
    ORDER BY
        1,
        3
),
decoded_response AS (
    SELECT
        block_id,
        ARRAY_TO_STRING(ARRAY_AGG(hex) within GROUP (
    ORDER BY
        INDEX ASC), '') AS decoded_response
    FROM
        try_decode_hex
    GROUP BY
        1
)
select
    r.block_id,
    r.DATA,
    TRY_PARSE_JSON(livequery.utils.udf_hex_to_string(decoded_response)) as decoded_result,
    r.error
from response r
LEFT JOIN decoded_response d using (block_id)
$$

{% endset %}
{% do run_query(sql) %}
{% endmacro %}
