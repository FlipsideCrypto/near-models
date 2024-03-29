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
        atlas_nft_table_id AS ez_nft_contract_metrics_id,
        receiver_id,
        tokens,
        transfers_24h,
        transfers_3d,
        all_transfers,
        owners,
        transactions,
        mints,
        COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__atlas_nft_table') }}
)
SELECT
    *
FROM
    nft_data
