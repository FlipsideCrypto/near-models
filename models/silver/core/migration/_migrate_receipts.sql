{{ config(
    'materialized' = 'ephemeral'
) }}

SELECT
    chunk_hash,
    block_id,
    block_timestamp,
    tx_hash,
    COALESCE(
        receipt_id,
        receipt_object_id
    ) AS receipt_id,
    receipt_actions AS receipt_json,
    execution_outcome AS outcome_json,
    _partition_by_block_number,
    streamline_receipts_final_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('silver__streamline_receipts_final') }}

    {% if var("BATCH_MIGRATE") %}
    WHERE
        {{ partition_load_manual('no_buffer') }}
    {% endif %}
