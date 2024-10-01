{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    tags = ['curated','scheduled_non_core'],
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date']
) }}

WITH receipts AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        receipt_actions :predecessor_id :: STRING AS predecessor_id,
        status_value,
        logs,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
        AND receipt_succeeded
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
        _partition_by_block_number,
        _inserted_timestamp
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
