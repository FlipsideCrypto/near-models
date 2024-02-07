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
    tx_status, -- TO DEPRECATE
    COALESCE(
        streamline_transactions_final_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    transactions
