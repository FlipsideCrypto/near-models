{{ config(
    materialized = 'incremental',
    unique_key = 'receipt_object_id',
    cluster_by = ['_load_timestamp::date', 'block_timestamp::DATE'],
    tags = ['curated', 'horizon']
) }}

WITH all_horizon_receipts AS (

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
        AND (
            LOWER(signer_id) = 'nearhorizon.near'
            OR LOWER(receiver_id) = 'nearhorizon.near'
        )
        AND _partition_by_block_number >= 86000000)
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
        receipt_succeeded,
        logs,
        proof,
        metadata,
        _load_timestamp,
        _partition_by_block_number
    FROM
        all_horizon_receipts
