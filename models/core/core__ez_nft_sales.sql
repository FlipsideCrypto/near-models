{{ config(
    materialized = 'view',
    secure = true
) }}

WITH nft_sales AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_sales') }}
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    buyer,
    seller,
    tx_status,
    nft_project,
    nft_id
FROM
    nft_sales
