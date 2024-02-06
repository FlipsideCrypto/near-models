{{ config(
    materialized = 'ephemeral',
    tags = ['helper', 'receipt_map']
) }}

SELECT
    receipt_object_id,
    block_id,
    _partition_by_block_number,
    _inserted_timestamp
    {% if not var('IS_MIGRATION') %}
    , _modified_timestamp
    {% endif %}
FROM
    {{ target.database }}.silver.streamline_receipts_final
WHERE
    {{ "_inserted_timestamp" if var('IS_MIGRATION') else "_modified_timestamp" }} >= SYSDATE() - INTERVAL '3 days'
    AND (
        tx_hash IS NULL
        OR block_timestamp IS NULL
    )
