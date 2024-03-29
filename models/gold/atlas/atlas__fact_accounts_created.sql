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

SELECT
    atlas_account_created_id AS fact_accounts_created_id,
    DAY,
    wallets_created,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__atlas_accounts_created') }}
