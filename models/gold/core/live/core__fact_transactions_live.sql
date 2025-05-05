{{ config(
    materialized = var('LIVE_TABLE_MATERIALIZATION', 'view'),
    secure = false,
    tags = ['livetable','fact_transactions']
) }}

WITH core_tx AS (
    SELECT * FROM {{ ref('core__fact_transactions') }}
),
bronze_tx AS (
    SELECT * FROM {{ ref('bronze__FR_transactions') }}
)

SELECT
    tx_hash,
    block_id,
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
    fact_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    btx.data,
    btx.value,
    btx.partition_key

FROM
    core_tx ctx
JOIN bronze_tx btx
ON ctx.tx_hash = btx.data:transaction:hash
