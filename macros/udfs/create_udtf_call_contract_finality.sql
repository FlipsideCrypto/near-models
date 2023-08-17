{% macro create_UDTF_CALL_CONTRACT_FUNCTION() %}
    {% set sql %}
    {#
    EXECUTE A method
    ON A deployed near smart contract USING THE `finality` block PARAMETER BY DEFAULT.signature STRING,
    STRING,
    OBJECT #}
    CREATE
    OR REPLACE FUNCTION {{ target.database }}.silver.udtf_call_contract_function(
        contract_address STRING,
        method_name STRING,
        args OBJECT
    ) returns TABLE (
        block_height NUMBER,
        DATA variant,
        decoded_result variant,
        error VARIANT
    ) AS $$ WITH params AS (
        SELECT
            LOWER(contract_address) AS contract_address,
            LOWER(method_name) AS method,
            'final' AS finality,
            BASE64_ENCODE(
                args :: STRING
            ) AS arg_base64
    ),
    call_function AS (
        SELECT
        ethereum.streamline.udf_api(
            'POST',
            'https://rpc.mainnet.near.org',
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
            res :data :: variant AS DATA,
            res :data :result :block_height :: NUMBER AS block_height,
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
SELECT
    r.block_height,
    r.data,
    TRY_PARSE_JSON(
        livequery.utils.udf_hex_to_string(decoded_response)
    ) AS decoded_result,
    r.error
FROM
    response r
    LEFT JOIN decoded_response d USING (block_height) $$ {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
