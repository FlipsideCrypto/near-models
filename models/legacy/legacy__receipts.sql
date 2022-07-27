{{ config(
    materialized = 'view',
    secure = true
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__receipts') }}
)
SELECT
    block_timestamp,
    block_hash,
    tx_hash AS txn_hash,
    receipt_object_id,
    receipt_outcome_id,
    status_value,
    logs,
    proof,
    metadata
FROM
    receipts
