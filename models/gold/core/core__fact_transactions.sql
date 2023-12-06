{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}
)
SELECT
    tx_hash,
    block_id,
    block_hash,
    block_timestamp,
    nonce,
    signature,
    tx_receiver,
    tx_signer,
    tx,
    gas_used,
    transaction_fee,
    attached_gas,
    tx_succeeded,
    tx_status
FROM
    transactions
