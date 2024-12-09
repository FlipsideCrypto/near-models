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
    receipt_object_id AS receipt_id,
    receipt_outcome_id,
    receiver_id,
    actions :predecessor_id :: STRING AS predecessor_id,
    signer_id,
    receipt_actions AS actions,
    execution_outcome AS outcome,
    gas_burnt,
    status_value,
    logs,
    proof,
    metadata,
    receipt_succeeded,
    receipt_object_id,
    COALESCE(
        streamline_receipts_final_id,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id']
        ) }}
    ) AS fact_receipts_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    receipts
