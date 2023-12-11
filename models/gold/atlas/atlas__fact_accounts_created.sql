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

WITH nft_data AS (

    SELECT
        atlas_account_created_id AS fact_accounts_created_id,
        DAY,
        wallets_created,
        COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__atlas_accounts_created') }}
)
SELECT
    *
FROM
    nft_data
