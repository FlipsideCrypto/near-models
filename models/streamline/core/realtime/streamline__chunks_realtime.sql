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
            "sql_limit": "60000",
            "producer_batch_size": "30000",
            "worker_batch_size": "7500",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_id DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}

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
        A.block_id,
        A.block_timestamp_epoch,
        A.chunk_hash
    FROM
        {{ ref('streamline__chunks') }} A
        LEFT JOIN {{ ref('streamline__chunks_complete') }} B ON A.chunk_hash = B.chunk_hash
    WHERE
        (
            A.block_id >= (
                SELECT
                    block_id
                FROM
                    last_3_days
            )
        )
        AND B.chunk_hash IS NULL
)
SELECT
    block_id,
    block_timestamp_epoch,
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
