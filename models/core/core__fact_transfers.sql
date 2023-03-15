{{ config(
    materialized = 'view',
    secure = true,
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
    status
FROM
    transfers

