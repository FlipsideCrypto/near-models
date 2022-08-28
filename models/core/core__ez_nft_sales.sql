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
    buyer,
    seller,
    tx_status,
    nft_project,
    nft_id,
    network_fee
FROM
    nft_sales
