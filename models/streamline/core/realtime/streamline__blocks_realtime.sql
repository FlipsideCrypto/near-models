-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks_v2",
            "sql_limit": "3000",
            "producer_batch_size": "3000",
            "worker_batch_size": "3000",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_number DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}
-- Note, roughly 3,000 blocks per hour (~70k/day).
-- batch sizing is WIP
WITH last_3_days AS (

    SELECT
        ZEROIFNULL(block_number) AS block_number
    FROM
        {{ ref("_block_lookback") }}
),
tbl AS (
    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks') }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
        -- TODO there may be skipped block heights! use hash / parent hash instead
        -- or will a skipped block height return a unique response that i can log
    SELECT
        block_number
    FROM
        {{ ref('streamline__blocks_complete') }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND block_hash IS NOT NULL
)
SELECT
    block_number,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
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
            'method',
            'block',
            'id',
            'Flipside/getBlock/0.1',
            'params',
            OBJECT_CONSTRUCT(
                'block_id',
                block_number
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM
    tbl
