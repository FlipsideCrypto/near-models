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
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__atlas_nft_30_trailing') }}
)
SELECT
    *
FROM
    TRAILING
