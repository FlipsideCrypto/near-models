{{ config(
    materialized = 'view',
    secure = true
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_index,
    receipt_object_id,
    receipt_outcome_id,
    receiver_id,
    receipt_actions AS actions,
    execution_outcome AS outcome,
    gas_burnt,
    status_value,
    logs,
    proof,
    metadata
FROM
    receipts
WHERE
    block_id <= (
        SELECT
            MAX(block_id)
        FROM
            receipts
    ) - 50
