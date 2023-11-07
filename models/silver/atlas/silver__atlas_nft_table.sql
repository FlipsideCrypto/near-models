

{{ config(
    unique_key = 'receiver_id',
    materialized = 'view',
    tags = ['atlas']
) }}

WITH nft_data AS (
    SELECT
        *
    FROM {{ ref('silver__atlas_nft_transactions') }}
)

select
  receiver_id,
  count(distinct token_id) as tokens,
  count(case when method_name = 'nft_transfer' and day >= (SYSDATE()::DATE - interval '1 day')
    then tx_hash end) as transfers_24h,
  count(case when method_name = 'nft_transfer' and day >= ( SYSDATE()::DATE - interval '3 day') 
    then tx_hash end) as transfers_3d,
  count(case when method_name = 'nft_transfer' then tx_hash end) as all_transfers,
  count(distinct owner) as owners,
  count(*) as transactions,
  count(case when method_name != 'nft_transfer' then tx_hash end) as mints
from nft_data
group by 1
order by 3 desc