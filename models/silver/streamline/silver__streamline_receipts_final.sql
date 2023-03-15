{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'receipt_object_id',
    cluster_by = ['_load_timestamp::date', 'block_id'],
    tags = ['receipt_map']
) }}

WITH base_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ partition_batch_load(150000) }}
            OR {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
blocks AS (
    SELECT
        block_id,
        block_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}
),
append_tx_hash AS (
    SELECT
        m.tx_hash,
        r.receipt_id,
        r.block_id,
        r.shard_id,
        r.receipt_index,
        r.chunk_hash,
        r.receipt,
        r.execution_outcome,
        r.outcome_receipts,
        r.receiver_id,
        r.signer_id,
        r.receipt_type,
        r._load_timestamp,
        r._partition_by_block_number
    FROM
        base_receipts r
        LEFT JOIN {{ ref('silver__receipt_tx_hash_mapping') }}
        m USING (receipt_id)
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_id AS receipt_object_id,
        r.block_id,
        b.block_timestamp,
        receipt_index,
        chunk_hash,
        receipt AS receipt_actions,
        execution_outcome,
        outcome_receipts AS receipt_outcome_id,
        receiver_id,
        signer_id,
        receipt_type,
        execution_outcome :outcome :gas_burnt :: NUMBER AS gas_burnt,
        execution_outcome :outcome :status :: variant AS status_value,
        execution_outcome :outcome :logs :: ARRAY AS logs,
        execution_outcome :proof :: ARRAY AS proof,
        execution_outcome :outcome :metadata :: variant AS metadata,
        _load_timestamp,
        _partition_by_block_number
    FROM
        append_tx_hash r
        LEFT JOIN blocks b USING (block_id)
)
SELECT
    *
FROM
    FINAL
