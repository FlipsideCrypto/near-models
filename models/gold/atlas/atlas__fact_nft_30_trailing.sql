{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}


WITH 30_trailing AS (
    SELECT
        day,
        txns
    FROM {{ ref('silver__atlas_nft_transactions') }}
)

SELECT 
    *
FROM 30_trailing