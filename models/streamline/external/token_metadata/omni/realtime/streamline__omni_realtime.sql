-- depends_on: {{ ref('streamline__omni_tokenlist') }}
{{ config(
    materialized = 'view',
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "omni_metadata",
            "sql_limit": "30",
            "producer_batch_size": "5",
            "worker_batch_size": "5",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['streamline_non_core']
) }}

WITH omni_token AS (
    SELECT
        contract_address
    FROM
        {{ ref('streamline__omni_tokenlist') }}
),
api_call AS (
    SELECT
        contract_address,
        response:data:result:result AS res_array
    FROM (
        SELECT
            contract_address,
            NEAR.LIVE.UDF_API(
                'POST',
                'https://rpc.mainnet.near.org',
                OBJECT_CONSTRUCT('Content-Type', 'application/json'),
                OBJECT_CONSTRUCT(
                    'jsonrpc', '2.0',
                    'id', 'dontcare',
                    'method', 'query',
                    'params', OBJECT_CONSTRUCT(
                        'request_type', 'call_function',
                        'finality', 'final',
                        'account_id', 'omni.bridge.near',
                        'method_name', 'get_token_id',
                        'args_base64', BASE64_ENCODE(OBJECT_CONSTRUCT('address', contract_address)::STRING)
                    )
                )
            ) AS response
        FROM omni_token
    )
),
flat AS (
    SELECT
        contract_address,
        VALUE::INT AS byte_val,
        LPAD(TO_CHAR(VALUE::INT, 'XX'), 2, '0') AS hex_val,
        INDEX AS idx
    FROM 
        api_call,
        LATERAL FLATTEN(input => res_array)
),
agg AS (
    SELECT
        contract_address,
        LISTAGG(hex_val, '') WITHIN GROUP (ORDER BY idx) AS hex_string
    FROM 
        flat
    GROUP BY 1
),
final AS (
    SELECT
    contract_address,
    hex_string,
    TRY_PARSE_JSON(
        livequery.utils.udf_hex_to_string(hex_string)
    ) AS decoded_result
    FROM 
        agg
)
SELECT 
    contract_address,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    OBJECT_CONSTRUCT(
        'contract_address', contract_address,
        'decoded_result', decoded_result
    ) AS request
FROM
    final
