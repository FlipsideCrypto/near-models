{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['core', 'nft']
) }}

WITH nft_sales AS (

    SELECT
        receipt_id,
        block_id,
        block_timestamp,
        tx_hash,
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        price,
        price_usd,
        method_name,
        log,
        gas_burned,
        affiliate_id,
        affiliate_amount,
        affiliate_amount_usd,
        royalties,
        platform_fee,
        platform_fee_usd,
        nft_sales_id AS ez_nft_sales_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__nft_complete_nft_sales') }}
)
SELECT
    *
FROM
    nft_sales
