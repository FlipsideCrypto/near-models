{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'receipt_object_id',
    cluster_by = ['modified_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social','scheduled_non_core']
) }}

WITH all_social_receipts AS (

    SELECT
        tx_hash,
        receipt_id AS receipt_object_id,
        block_id,
        block_timestamp,
        NULL AS receipt_index,
        chunk_hash,
        receipt_json AS receipt_actions,
        outcome_json AS execution_outcome,
        outcome_json :outcome :receipt_ids :: ARRAY AS receipt_outcome_id,
        receiver_id,
        predecessor_id,
        receipt_json :receipt :Action :signer_id :: STRING AS signer_id,
        NULL AS receipt_type,
        outcome_json :outcome :gas_burnt :: NUMBER AS gas_burnt,
        outcome_json :outcome :status :: VARIANT AS status_value,
        outcome_json :outcome :logs :: ARRAY AS logs,
        outcome_json :proof :: ARRAY AS proof,
        outcome_json :outcome :metadata :: VARIANT AS metadata,
        _partition_by_block_number
    FROM
        {{ ref('silver__receipts_final') }}
    WHERE
        (
            LOWER(signer_id) = 'social.near'
            OR LOWER(receiver_id) = 'social.near'
        )

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
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
)
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id']
        ) }} AS social_receipts_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        all_social_receipts
