{{ config(
    materialized = 'view',
    tags = ['streamline_helper']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'jsonrpc',
            '2.0',
            'id',
            'dontcare',
            'method',
            'status',
            'params',
            OBJECT_CONSTRUCT(
                'finality',
                'final'
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) :data :result :sync_info :latest_block_height :: INT AS block_id
