-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
-- depends on: {{ ref('streamline__transactions') }}
-- depends on: {{ ref('streamline__transactions_complete') }}

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "transactions_v2",
            "sql_limit": "300000",
            "producer_batch_size": "75000",
            "worker_batch_size": "25000",
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
        A.block_id,
        A.block_timestamp_epoch,
        A.tx_hash,
        A.signer_id,
        A.shard_id,
        A.chunk_hash,
        A.height_created,
        A.height_included
    FROM
        {{ ref('seeds__impacted_blocks_060325') }} C
        LEFT JOIN {{ ref('streamline__transactions') }} A ON A.block_id = C.block_id
        LEFT JOIN {{ ref('streamline__transactions_complete') }} B ON A.tx_hash = B.tx_hash
    WHERE
        A.tx_hash IS NOT NULL
        AND B.tx_hash IS NULL
)
{% else %}
{% if var('STREAMLINE_BACKFILL', false) %}
last_3_days AS (
    SELECT
        140750000 as block_id
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
        A.tx_hash,
        A.signer_id,
        A.shard_id,
        A.chunk_hash,
        A.height_created,
        A.height_included
    FROM
        {{ ref('streamline__transactions') }} A
        LEFT JOIN {{ ref('streamline__transactions_complete') }} B ON A.tx_hash = B.tx_hash
    WHERE
        (
            A.block_id >= (
                SELECT
                    block_id
                FROM
                    last_3_days
            )
        )
        AND A.signer_id IS NOT NULL
        AND B.tx_hash IS NULL
)
{% endif %}
SELECT
    shard_id,
    chunk_hash,
    block_id,
    block_timestamp_epoch,
    height_created,
    height_included,
    tx_hash,
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
            'EXPERIMENTAL_tx_status',
            'id',
            'Flipside/getTransactionWithStatus/' || request_timestamp || '/' || tx_hash :: STRING,
            'params',
            OBJECT_CONSTRUCT(
                'tx_hash',
                tx_hash,
                'sender_account_id',
                signer_id,
                'wait_until',
                'FINAL'
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM
    tbl
