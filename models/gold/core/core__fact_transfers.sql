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
    block_id,
    block_timestamp,
    action_id,
    deposit,
    tx_hash,
    tx_signer,
    tx_receiver,
    receipt_object_id,
    signer_id,
    receiver_id,
    transaction_fee,
    gas_used,
    tx_succeeded,
    receipt_succeeded,
    status,
    COALESCE(
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['action_id']
        ) }}
    ) AS fact_transfers_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    transfers
