-- depends_on: {{ ref('bronze__chunks') }}
-- depends_on: {{ ref('bronze__FR_chunks') }}
-- depends on: {{ ref('streamline__chunks') }}
-- depends_on: {{ ref('streamline__chunks_complete') }}

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "chunks_v2",
            "sql_limit": "210000",
            "producer_batch_size": "42000",
            "worker_batch_size": "21000",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_id ASC"
        }
    ),
    tags = ['streamline_realtime']
) }}
-- Note, roughly 3,000 blocks per hour (~70k/day) * 6 chunks per block (for now)
-- batch sizing is WIP
WITH
{% if var('STREAMLINE_PARTIAL_BACKFILL', false) %}
last_3_days AS (
    SELECT
        120939872 as block_id
),
{% else %}
last_3_days AS (

    SELECT
        ZEROIFNULL(block_id) AS block_id
    FROM
        {{ ref("_block_lookback") }}
),
{% endif %}
tbl AS (
    SELECT
        block_id,
        chunk_hash
    FROM
        {{ ref('streamline__chunks') }}
    WHERE
        (
            block_id >= (
                SELECT
                    block_id
                FROM
                    last_3_days
            )
        )
        AND chunk_hash IS NOT NULL
    EXCEPT
    SELECT
        block_id,
        chunk_hash
    FROM
        {{ ref('streamline__chunks_complete') }}
    WHERE
        block_id >= (
            SELECT
                block_id
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
    block_id,
    FLOOR(block_id, -3) AS partition_key,
    chunk_hash,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS request_timestamp,
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
            'Flipside/getChunk/' || request_timestamp || '/' || chunk_hash :: STRING,
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
