{{ config(
    materialized = 'ephemeral'
) }}

WITH lake_transactions_final AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        block_hash,
        tx_succeeded,
        gas_used,
        transaction_fee,
        attached_gas,
        _partition_by_block_number,
        streamline_transactions_final_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if var("BATCH_MIGRATE") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% endif %}
),
lake_transactions_int AS (
    SELECT
        tx_hash,
        block_id,
        shard_number,
        chunk_hash,
        tx :transaction :: variant AS transaction_json,
        tx :outcome :execution_outcome :: variant AS outcome_json,
        _partition_by_block_number
    FROM
        {{ ref('silver__streamline_transactions') }}

        {% if var("BATCH_MIGRATE") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% endif %}
),
transaction_archive AS (
    SELECT
        i.chunk_hash,
        i.shard_number AS shard_id,
        f.block_hash,
        f.block_id,
        f.block_timestamp,
        f.tx_hash,
        i.transaction_json,
        i.outcome_json,
        f.tx_succeeded,
        f.gas_used,
        f.transaction_fee,
        f.attached_gas,
        f._partition_by_block_number
    FROM
        lake_transactions_final f
        LEFT JOIN lake_transactions_int i
        ON f.tx_hash = i.tx_hash
        AND f._partition_by_block_number = i._partition_by_block_number
)
SELECT
    *
FROM
    transaction_archive
