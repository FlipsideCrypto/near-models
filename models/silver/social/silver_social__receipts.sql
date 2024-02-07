{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'receipt_object_id',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'social']
) }}

WITH all_social_receipts AS (

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
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
        {% endif %}
        AND (
            LOWER(signer_id) = 'social.near'
            OR LOWER(receiver_id) = 'social.near'
        )
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
