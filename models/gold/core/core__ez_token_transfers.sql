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
    raw_amount,
    raw_amount_precise,
    amount,
    amount_usd,
    transfer_type,
    symbol,
    token_price,
    has_price,
    COALESCE(
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['transfers_id']
        ) }}
    ) AS fact_token_transfers_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    token_transfers
