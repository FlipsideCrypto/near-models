{{ config(
    materialized = 'ephemeral'
) }}

WITH lake_receipts_final AS (

    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        tx_hash,
        COALESCE(
            receipt_id,
            receipt_object_id
        ) AS receipt_id,
        COALESCE(
            predecessor_id,
            receipt_actions :predecessor_id :: STRING
        ) AS predecessor_id,
        receiver_id,
        receipt_actions AS receipt_json,
        execution_outcome AS outcome_json,
        receipt_succeeded,
        _partition_by_block_number,
        {{ dbt_utils.generate_surrogate_key(
            ['COALESCE(receipt_id, receipt_object_id)']
        ) }} AS receipts_final_id,
        COALESCE(
            inserted_timestamp,
            _inserted_timestamp,
            _load_timestamp
        ) AS inserted_timestamp,
        COALESCE(
            modified_timestamp,
            _inserted_timestamp,
            _load_timestamp
        ) AS modified_timestamp,
        _invocation_id
    FROM
        {{ ref('silver__streamline_receipts_final') }}

        {% if var("NEAR_MIGRATE_ARCHIVE") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% endif %}
),
lake_transactions_final AS (
    SELECT
        tx_hash,
        tx_succeeded
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if var("NEAR_MIGRATE_ARCHIVE") %}
        WHERE
            {{ partition_load_manual('front') }}
        {% endif %}
)
SELECT
    chunk_hash,
    block_id,
    block_timestamp,
    r.tx_hash,
    receipt_id,
    predecessor_id,
    receiver_id,
    receipt_json,
    outcome_json,
    tx_succeeded,
    receipt_succeeded,
    _partition_by_block_number,
    receipts_final_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    lake_receipts_final r
    LEFT JOIN lake_transactions_final tx
    ON r.tx_hash = tx.tx_hash
