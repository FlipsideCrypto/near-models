{{ config(
    materialized = 'view'
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
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
    attached_gas
FROM
    transactions
