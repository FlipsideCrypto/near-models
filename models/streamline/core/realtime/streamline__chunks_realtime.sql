-- depends_on: {{ ref('bronze__chunks') }}
-- depends_on: {{ ref('bronze__FR_chunks') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "chunks_v2",
            "sql_limit": "18000",
            "producer_batch_size": "6000",
            "worker_batch_size": "6000",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_number DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}
-- Note, roughly 3,000 blocks per hour (~70k/day) * 6 chunks per block (for now)
-- batch sizing is WIP
WITH last_3_days AS (

    SELECT
        ZEROIFNULL(block_number) AS block_number
    FROM
        {{ ref("_block_lookback") }}
),
tbl AS (
    SELECT
        block_number,
        chunk_hash
    FROM
        {{ ref('streamline__chunks') }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND chunk_hash IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        chunk_hash
    FROM
        {{ ref('streamline__chunks_complete') }}
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
        AND chunk_hash IS NOT NULL
)
SELECT
    block_number,
    chunk_hash,
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
            'chunk',
            'id',
            'Flipside/getChunk/0.1',
            'params',
            OBJECT_CONSTRUCT(
                'chunk_id',
                chunk_hash
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM
    tbl
