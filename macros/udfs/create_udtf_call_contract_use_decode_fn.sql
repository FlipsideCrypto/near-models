{% macro create_udf_call_contract_function_3() %}
{% set sql %}
{# 
    Same as decode with finality, but uses the python version of the bytelist decoder.
 #}
CREATE OR REPLACE FUNCTION
    {{ target.database }}.SILVER.UDTF_CALL_CONTRACT_3(
        contract_address STRING,
        method_name STRING,
        finality STRING,
        args STRING
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
        BASE64_ENCODE(args) AS arg_base64
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
)
SELECT
    res :data :result :block_height :: NUMBER AS block_height,
    res :data ::VARIANT as data,
    {{ target.database }}.silver.udf_decode_bytelist(
        res :data :result :result :: ARRAY) AS DECODED_RESULT,
    res :data :result :error ::STRING as error
FROM
    call_function

$$

{% endset %}
{% do run_query(sql) %}
{% endmacro %}
