{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['core', 'nft']
) }}


WITH token_transfers AS (

    SELECT
        *
    FROM
        {{ ref('silver__token_transfers') }}
    WHERE
        transfer_type = 'nft'
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    action_id,
    contract_address,
    from_address,
    to_address,
    memo as token_id,
    COALESCE(
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['transfers_id']
        ) }}
    ) AS fact_nft_transfers_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    token_transfers
