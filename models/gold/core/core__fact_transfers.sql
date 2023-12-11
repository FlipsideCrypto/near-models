{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__transfers_s3') }}
)
SELECT
    tx_hash,
    action_id,
    block_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_object_id,
    transaction_fee,
    gas_used,
    status,
    COALESCE(
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id']
        ) }}
    ) AS fact_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    transfers
