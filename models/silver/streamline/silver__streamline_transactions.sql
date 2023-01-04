{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'tx_hash',
    cluster_by = ['_load_timestamp::date', 'block_id', 'tx_hash']
) }}

WITH chunks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        ARRAY_SIZE(chunk_transactions) > 0
        AND {{ incremental_load_filter('_load_timestamp') }}
),
txs AS (
    SELECT
        VALUE :transaction :hash :: STRING AS tx_hash,
        block_id,
        shard_id,
        INDEX AS transactions_index,
        _load_timestamp,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        VALUE as tx
    FROM
        chunks,
        LATERAL FLATTEN(
            input => chunk_transactions
        )
)
SELECT
    *
FROM
    txs
