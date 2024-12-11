{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST._partition_by_block_number >= (select min(_partition_by_block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'tx_hash',
    cluster_by = ['modified_timestamp::date', '_partition_by_block_number'],
    tags = ['load', 'load_shards','scheduled_core']
) }}

WITH chunks AS (

    SELECT
        block_id,
        shard_id,
        chunk,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        array_size(chunk :transactions :: ARRAY) > 0

    {% if var('MANUAL_FIX') %}
        AND
            {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
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
        _inserted_timestamp
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
        tx :transaction :actions :: VARIANT AS _actions,
        tx :transaction :hash :: STRING AS _hash,
        tx :transaction :nonce :: STRING AS _nonce,
        tx :outcome :execution_outcome :: VARIANT AS _outcome,
        tx :transaction :public_key :: STRING AS _public_key,
        [] AS _receipt,
        tx :transaction :receiver_id :: STRING AS _receiver_id,
        tx :transaction :signature :: STRING AS _signature,
        tx :transaction :signer_id :: STRING AS _signer_id,
        _partition_by_block_number,
        _inserted_timestamp
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
        _inserted_timestamp
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

QUALIFY(row_number() over (partition by tx_hash order by modified_timestamp desc)) = 1
