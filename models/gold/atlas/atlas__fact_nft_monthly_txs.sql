{{ config(
    materialized = 'view',
    secure = false,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'ATLAS'
            }
        }
    },
    tags = ['atlas']
) }}

WITH TRAILING AS (

    SELECT
        atlas_nft_30_trailing_id AS fact_nft_monthly_txs_id,
        DAY,
        txns,
        COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__atlas_nft_30_trailing') }}
)
SELECT
    *
FROM
    TRAILING
