-- depends_on: {{ ref('bronze__blocks') }}
-- depends_on: {{ ref('bronze__FR_blocks') }}
-- depends_on: {{ ref('streamline__blocks_complete') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks_v2",
            "sql_limit": "10000",
            "producer_batch_size": "5000",
            "worker_batch_size": "2500",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_id DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}

WITH 
{% if var('STREAMLINE_GAPFILL', false) %}
    tbl AS (
        SELECT
            block_id
        FROM
            {{ ref('seeds__impacted_blocks') }} A 
        LEFT JOIN {{ ref('streamline__blocks_complete') }} B ON A.block_id = B.block_id
        WHERE B.block_id IS NULL
    )
    {% else %}
    {% if var('STREAMLINE_BACKFILL', false) %}
        last_3_days AS (
            SELECT
                140750000 AS block_id
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
{% endif %}
SELECT
    block_id,
    FLOOR(block_id, -3) AS partition_key,
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
            'block',
            'id',
            'Flipside/getBlock/' || request_timestamp || '/' || block_id :: STRING,
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
