
{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}

WITH nft_data AS (
    SELECT
        day,
        wallets_created,
        total_wallets,
        inserted_timestamp,
        updated_timestamp
    FROM {{ ref('silver__atlas_accounts_created') }}
)

select
    *
from nft_data