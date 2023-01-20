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
    block_id,
    tx_hash,
    receipt_index,
    receipt_object_id,
    receipt_outcome_id,
    receiver_id,
    gas_burnt,
    status_value,
    logs,
    proof,
    metadata
FROM
    receipts
