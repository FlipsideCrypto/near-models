{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH token_transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_transfers') }}
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    contract_address,
    from_address,
    to_address,
    memo,
    amount_raw,
    amount_raw_precise,
    transfer_type,
    transfers_id AS fact_token_transfers_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    token_transfers
