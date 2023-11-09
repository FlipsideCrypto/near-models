
{{ config(
    materialized = 'view',
    secure = false,
    tags = ['atlas']
) }}

WITH nft_data AS (
    SELECT
        id as ez_nft_contract_metrics_id,
        receiver_id,
        tokens,
        transfers_24h,
        transfers_3d,
        all_transfers,
        owners,
        transactions,
        mints,
        inserted_timestamp,
        modified_timestamp
    FROM {{ ref('silver__atlas_nft_table') }}
)

select
    *
from nft_data