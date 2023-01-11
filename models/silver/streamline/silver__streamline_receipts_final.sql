{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'receipt_id',
    cluster_by = ['_load_timestamp::date', 'block_id']
) }}

WITH base_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts') }}

{% if is_incremental() %}
WHERE
    _partition_by_block_number BETWEEN (
        SELECT
            MAX(_partition_by_block_number)
        FROM
            {{ this }}
    )
    AND (
        (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                {{ this }}
        ) + 250000
    )
{%- else -%}
{# TODO - reset this or just use the macro. Changed the block range to match dev sample size #}
WHERE
    _partition_by_block_number BETWEEN 79000000
    AND 79100000
{% endif %}
AND {{ incremental_load_filter('_load_timestamp') }}
),
append_tx_hash AS (
    SELECT
        m.tx_hash,
        r.receipt_id,
        r.block_id,
        r.shard_id,
        r.source_object,
        r.receipt_index,
        r.chunk_hash,
        r.receipt,
        r.execution_outcome,
        r.outcome_receipts,
        r.receiver_id,
        r.signer_id,
        r.receipt_type,
        r._load_timestamp,
        r._partition_by_block_number
    FROM
        base_receipts r
        LEFT JOIN {{ ref('silver__receipt_tx_hash_mapping') }}
        m USING (receipt_id)
)
SELECT
    *
FROM
    append_tx_hash
