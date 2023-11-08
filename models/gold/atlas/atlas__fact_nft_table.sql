
{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}

WITH nft_data AS (
    SELECT
        receiver_id,
        tokens,
        ransfers_24h,
        transfers_3d,
        all_transfers,
        owners,
        transactions,
        mints
    FROM {{ ref('silver__atlas_nft_table') }}
)

select
    *
from nft_data