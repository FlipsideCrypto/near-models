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
    txn_hash,
    block_height,
    block_hash,
    block_timestamp,
    nonce,
    signature,
    tx_receiver,
    tx_signer,
    tx,
    tx :receipt AS tx_receipt,
    tx :outcome AS tx_outcome,
    gas_used,
    transaction_fee,
    attached_gas,
    _ingested_at AS ingested_at
FROM
    transactions
