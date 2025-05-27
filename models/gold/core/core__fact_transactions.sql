{{ config(
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    secure = false,
    tags = ['scheduled_core']
) }}

SELECT
    tx_hash,
    block_id,
    block_timestamp,
    transaction_json :nonce :: INT AS nonce,
    transaction_json :signature :: STRING AS signature,
    tx_receiver,
    tx_signer,
    transaction_json AS tx,
    gas_used,
    transaction_fee,
    attached_gas,
    tx_succeeded,
    transactions_final_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__transactions_final') }}
