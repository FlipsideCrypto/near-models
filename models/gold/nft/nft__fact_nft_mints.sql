{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['core', 'nft']
) }}

WITH nft_mints AS (

    SELECT
        receipt_object_id,
        tx_hash,
        block_id,
        block_timestamp,
        token_id,
        method_name,
        args,
        memo,
        deposit,
        tx_receiver,
        receiver_id,
        signer_id,
        owner_id,
        owner_per_tx,
        mint_per_tx,
        gas_burnt,
        transaction_fee,
        implied_price,
        tx_status,
        mint_action_id,
        COALESCE(
            standard_nft_mint_id,
            {{ dbt_utils.generate_surrogate_key(
                ['mint_action_id']
            ) }}
        ) AS fact_nft_mints_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__standard_nft_mint_s3') }}
)
SELECT
    *
FROM
    nft_mints
