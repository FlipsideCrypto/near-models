{{ config(
    materialized = 'ephemeral',
    tags = ['helper', 'receipt_map','scheduled_core']
) }}

SELECT
    receipt_object_id,
    block_id,
    _partition_by_block_number,
    _inserted_timestamp
FROM
    {{ target.database }}.silver.streamline_receipts_final
WHERE
    _inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
    AND (
        tx_hash IS NULL
        OR block_timestamp IS NULL
    )
