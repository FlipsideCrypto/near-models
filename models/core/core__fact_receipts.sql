{{ config(
    materialized = 'view'
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
    receipt_object_id,
    receipt_outcome_id,
    status_value,
    logs,
    proof,
    metadata
FROM
    receipts
