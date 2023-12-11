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

WITH nft_detailed AS (

    SELECT
        atlas_nft_detailed_id AS ez_nft_contract_metrics_daily_id,
        DAY,
        receiver_id,
        tokens,
        all_transfers,
        owners,
        transactions,
        mints,
        COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__atlas_nft_detailed') }}
)
SELECT
    *
FROM
    nft_detailed
