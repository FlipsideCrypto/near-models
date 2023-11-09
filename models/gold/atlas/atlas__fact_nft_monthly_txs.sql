{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}


WITH trailing AS (
    SELECT
        id as fact_nft_monthly_txs_id,
        day,
        txns,
        inserted_timestamp,
        modified_timestamp
    FROM {{ ref('silver__atlas_nft_30_trailing') }}
)

SELECT 
    *
FROM trailing