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
    EXCEPT
    SELECT
        contract_address
    FROM
        {{ ref('streamline__omni_complete')}}
)
SELECT
    contract_address,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.LIVE.UDF_API(
        'POST',
        '{Service}',
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
                'args_base64', BASE64_ENCODE(OBJECT_CONSTRUCT('address', contract_address) :: STRING)
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM
    omni_token