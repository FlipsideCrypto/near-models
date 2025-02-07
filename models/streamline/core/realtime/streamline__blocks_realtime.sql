-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks_v2",
            "sql_limit": "490000",
            "producer_batch_size": "70000",
            "worker_batch_size": "35000",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_id DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}
-- Note, roughly 3,000 blocks per hour (~70k/day).
-- batch sizing is WIP

WITH 
{% if var('STREAMLINE_PARTIAL_BACKFILL', false) %}
last_3_days AS (
    SELECT
        120960000 as block_id
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
        block_id
    FROM
        {{ ref('streamline__blocks') }}
    WHERE
        (
            block_id >= (
                SELECT
                    block_id
                FROM
                    last_3_days
            )
        )
        AND block_id IS NOT NULL
    EXCEPT
        -- TODO there may be skipped block heights! use hash / parent hash instead
        -- or will a skipped block height return a unique response that i can log
    SELECT
        block_id
    FROM
        {{ ref('streamline__blocks_complete') }}
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
        AND block_hash IS NOT NULL
)
SELECT
    block_id,
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
            'Flipside/getBlock/' || block_id :: STRING,
            'params',
            OBJECT_CONSTRUCT(
                'block_id',
                block_id
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM
    tbl
