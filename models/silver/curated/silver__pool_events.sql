{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    tags = ['curated'],
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date']
) }}

WITH receipts AS (

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
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        status_value,
        logs,
        VALUE AS LOG,
        _load_timestamp,
        _partition_by_block_number
    FROM
        receipts,
        LATERAL FLATTEN(logs)
    WHERE
        ARRAY_SIZE(logs) > 0
        AND receiver_id ILIKE '%.pool%.near'
)
SELECT
    *
FROM
    FINAL
