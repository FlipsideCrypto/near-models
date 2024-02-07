{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'tx_hash',
    cluster_by = ['_inserted_timestamp::date'],
    tags = ['load', 'load_shards']
) }}

WITH chunks AS (

    SELECT
        block_id,
        shard_id,
        chunk,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        chunk != 'null'
    {% if var('IS_MIGRATION') %}
        AND
            _inserted_timestamp >= (
                SELECT 
                    MAX(_inserted_timestamp) - INTERVAL '{{ var('STREAMLINE_LOAD_LOOKBACK_HOURS') }} hours'
                FROM {{ this }}
            )
    {% else %}
        AND {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
),
flatten_transactions AS (
    SELECT
        VALUE :transaction :hash :: STRING AS tx_hash,
        block_id,
        shard_id,
        INDEX AS transactions_index,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        VALUE :outcome :execution_outcome :outcome :receipt_ids :: ARRAY AS outcome_receipts,
        VALUE AS tx,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        chunks,
        LATERAL FLATTEN(
            input => chunk :transactions :: ARRAY
        )
),
txs AS (
    SELECT
        tx_hash,
        block_id,
        shard_id,
        transactions_index,
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
        tx :transaction :signer_id :: STRING AS _signer_id,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        txs
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
