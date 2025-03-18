{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'tx_hash',
    tags = ['curated','scheduled_non_core'],
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date']
) }}

WITH receipts AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_id AS receipt_object_id,
        receiver_id,
        receipt_json :receipt :Action :signer_id :: STRING AS signer_id,
        predecessor_id,
        receipt_json AS receipt_actions,
        outcome_json :outcome :status :: VARIANT AS status_value,
        outcome_json :outcome :logs :: ARRAY AS logs,
        _partition_by_block_number
    FROM
        {{ ref('silver__receipts_final') }}
    WHERE
        receipt_succeeded
        {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}

        {% elif is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        predecessor_id,
        status_value,
        logs,
        VALUE AS LOG,
        _partition_by_block_number
    FROM
        receipts,
        LATERAL FLATTEN(logs)
    WHERE
        ARRAY_SIZE(logs) > 0
        AND receiver_id ILIKE '%.pool%.near'
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS pool_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
