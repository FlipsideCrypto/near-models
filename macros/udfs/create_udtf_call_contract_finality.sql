{% macro create_UDTF_CALL_CONTRACT_FUNCTION() %}
{% set sql %}
{# 
    Execute a method on a deployed NEAR smart contract using the `finality` block parameter.
 #}

CREATE OR REPLACE FUNCTION
    {{ target.database }}.SILVER.UDTF_CALL_CONTRACT_FUNCTION(
        contract_address STRING,
        method_name STRING,
        finality STRING,
        args OBJECT
    )
RETURNS TABLE (
    BLOCK_HEIGHT NUMBER,
    DATA VARIANT,
    DECODED_RESULT VARIANT,
    ERROR STRING
)
AS
$$
WITH params AS (
    SELECT
        lower(contract_address) AS contract_address,
        lower(method_name) AS method,
        lower(finality) as finality,
        BASE64_ENCODE(args::STRING) AS arg_base64
),
call_function AS (
    SELECT
        ethereum.streamline.udf_api(
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
                    'finality': finality,
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
        res,
        res :data ::VARIANT as data,
        res :data :result :block_height :: NUMBER AS block_height,
        res :data :result :result :: ARRAY AS res_array,
        res :data :result :error ::STRING as error
    FROM
        call_function
),
try_decode_hex AS (
    SELECT
        block_height,
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
        block_height,
        ARRAY_TO_STRING(ARRAY_AGG(hex) within GROUP (
    ORDER BY
        INDEX ASC), '') AS decoded_response
    FROM
        try_decode_hex
    GROUP BY
        1
)
select
    r.block_height,
    r.DATA,
    TRY_PARSE_JSON(livequery.utils.udf_hex_to_string(decoded_response)) as decoded_result,
    r.error
from response r
LEFT JOIN decoded_response d using (block_height)
$$

{% endset %}
{% do run_query(sql) %}
{% endmacro %}
