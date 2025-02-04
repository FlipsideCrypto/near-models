{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "blocks_v2",
            "sql_limit": "100",
            "producer_batch_size": "100",
            "worker_batch_size": "100",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['streamline_realtime']
) }}

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
        -- AND {} IS NOT NULL -- TODO, determine identifier for bad response
)
SELECT
    block_number,
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
            'id', 'Flipside/getBlock/0.1',
            'params', OBJECT_CONSTRUCT(
                'block_id',  block_number
            )
        ),
        'Vault/prod/near/quicknode/mainnet'
    ) AS request
FROM tbl
ORDER BY 
    block_number DESC
