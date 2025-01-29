{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks",
            "sql_limit": "100",
            "producer_batch_size": "100",
            "worker_batch_size": "100",
            "sql_source": "{{this.identifier}}"
        }
    )
) }}

-- single block for testing
SELECT
    138515000 AS block_number,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json'
        ),
        OBJECT_CONSTRUCT(
            'jsonrpc', '2.0',
            'method', 'block',
            'id', 'dontcare',
            'params', OBJECT_CONSTRUCT(
                'block_id',  block_number
            )
        ),
        'Vault/prod/near/quicknode/mainneet'
    ) AS request

