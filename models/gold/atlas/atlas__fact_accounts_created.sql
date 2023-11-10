{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}

WITH nft_data AS (

    SELECT
        atlas_account_created_id AS fact_accounts_created_id,
        DAY,
        wallets_created,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__atlas_accounts_created') }}
)
SELECT
    *
FROM
    nft_data
