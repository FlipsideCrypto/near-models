{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['core', 'nft']
) }}


WITH nft_token_transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_transfers') }}
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    receipt_id AS action_id,
    contract_address,
    from_address,
    to_address,
    token_id,
    nft_transfers_id AS fact_nft_transfers_id,
    COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    nft_token_transfers
