{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'tx_hash',
    cluster_by = ['_inserted_timestamp::date', 'block_id'],
    tags = ['load', 'load_shards']
) }}

WITH chunks AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_chunks') }}
    WHERE
        ARRAY_SIZE(chunk_transactions) > 0
        AND {{ incremental_load_filter('_inserted_timestamp') }}
),
flatten_transactions AS (
    SELECT
        VALUE :transaction :hash :: STRING AS tx_hash,
        block_id,
        shard_id,
        INDEX AS transactions_index,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp,
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
        tx_hash,
        block_id,
        shard_id,
        transactions_index,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp,
        chunk_hash,
        outcome_receipts,
        tx,
        tx :transaction :actions :: variant AS _actions,
        tx :transaction :hash :: STRING AS _hash,
        tx :transaction :nonce :: STRING AS _nonce,
        tx :outcome :execution_outcome :: variant AS _outcome,
        tx :transaction :public_key :: STRING AS _public_key,
        [] AS _receipt,
        tx :transaction :receiver_id :: STRING AS _receiver_id,
        tx :transaction :signature :: STRING AS _signature,
        tx :transaction :signer_id :: STRING AS _signer_id
    FROM
        flatten_transactions
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        shard_id,
        transactions_index,
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
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        txs qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS streamline_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
