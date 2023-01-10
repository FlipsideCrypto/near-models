{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'tx_hash',
    cluster_by = ['_load_timestamp::date', 'block_id', 'tx_hash']
) }}

{# TODO - look into optimizing a full run #}

WITH chunks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        ARRAY_SIZE(chunk_transactions) > 0
        AND {{ incremental_load_filter('_load_timestamp') }}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
),
flatten_transactions AS (
    SELECT
        VALUE :transaction :hash :: STRING AS tx_hash,
        block_id,
        shard_id,
        INDEX AS transactions_index,
        _load_timestamp,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        VALUE :outcome :execution_outcome :outcome :receipt_ids :: ARRAY AS outcome_receipts,
        VALUE AS tx
    FROM
        chunks,
        LATERAL FLATTEN(
            input => chunk_transactions
        )
),
txs AS (
    SELECT
        t.tx_hash,
        t.block_id,
        b.block_hash,
        b.block_timestamp,
        t.shard_id,
        t.transactions_index,
        t._load_timestamp as _tx_load_timestamp,
        b._load_timestamp as _block_load_timestamp,
        t.chunk_hash,
        t.outcome_receipts,
        t.tx,
        t.tx :transaction :actions :: variant as _actions,
        t.tx :transaction :hash :: STRING as _hash,
        t.tx:transaction:nonce::string as _nonce,
        t.tx :outcome :execution_outcome :: variant as _outcome,
        t.tx :transaction :public_key :: STRING as _public_key,
        [] as _receipt,
        t.tx :transaction :receiver_id :: STRING as _receiver_id,
        t.tx :transaction :signature :: STRING as _signature,
        t.tx :transaction :signer_id :: STRING as _signer_id
    FROM
        flatten_transactions t
        LEFT JOIN blocks b USING (block_id)
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_hash,
        block_timestamp,
        shard_id,
        transactions_index,
        _tx_load_timestamp,
        _block_load_timestamp,
        chunk_hash,
        outcome_receipts,
        tx,
        _actions,
        _hash,
        _nonce,
        _outcome,
        _public_key,
        _receipt,
        _receiver_id,
        _signature,
        _signer_id,
        _block_load_timestamp as _load_timestamp
    FROM
        txs
)
SELECT
    *
FROM
    FINAL
