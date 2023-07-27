{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    cluster_by = ['_load_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH all_social_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
        AND (LOWER(signer_id) = 'social.near'
        OR LOWER(receiver_id) = 'social.near')
        AND receipt_succeeded
        )
    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receipt_index,
        chunk_hash,
        receipt_actions,
        execution_outcome,
        receipt_outcome_id,
        receiver_id,
        signer_id,
        receipt_type,
        gas_burnt,
        status_value,
        logs,
        proof,
        metadata,
        _load_timestamp,
        _partition_by_block_number
    FROM
        all_social_receipts
