{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
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
    receipt_object_id,
    receipt_outcome_id,
    receiver_id,
    receipt_actions AS actions,
    execution_outcome AS outcome,
    gas_burnt,
    status_value,
    logs,
    proof,
    metadata,
    COALESCE(
        streamline_receipts_final_id,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id']
        ) }}
    ) AS fact_receipts_id,
    inserted_timestamp,
    modified_timestamp
FROM
    receipts
