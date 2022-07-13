{{ config(
    materialized = 'view'
) }}

WITH transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__transfers') }}
)
SELECT
    tx_hash AS txn_hash,
    action_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_object_id,
    transaction_fee,
    gas_used,
    status,
    _ingested_at AS ingested_at
FROM
    transfers
