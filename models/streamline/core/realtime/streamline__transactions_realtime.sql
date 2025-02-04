-- depends_on: {{ ref('bronze__transactions') }}
-- depends_on: {{ ref('bronze__FR_transactions') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "transactions_v2",
            "sql_limit": "100000",
            "producer_batch_size": "10000",
            "worker_batch_size": "10000",
            "sql_source": "{{this.identifier}}",
            "order_by_column": "block_number DESC"
        }
    ),
    tags = ['streamline_realtime']
) }}

-- Note, anywhere from 100k to >500k transactions per hour
-- batch sizing and scheduling is WIP

-- TODO reminder that we are waiting for FINAL status so retry on some will be required


WITH last_3_days AS (

    SELECT
        ZEROIFNULL(block_number) AS block_number
    FROM
        {{ ref("_block_lookback") }}
),
tbl AS (
    SELECT
        block_number,
        tx_hash,
        signer_id
    FROM
        {{ ref('streamline__transactions') }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND tx_hash IS NOT NULL
        AND signer_id IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        tx_hash,
        signer_id
    FROM
        {{ ref('streamline__transactions_complete') }}
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
        AND tx_hash IS NOT NULL
        AND signer_id IS NOT NULL
)
SELECT
    block_number,
    tx_hash,
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
            'EXPERIMENTAL_tx_status',
            'id',
            'Flipside/getTransactionWithStatus/0.1',
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
