-- TODO slated for deprecation and drop

{{ config(
  materialized = 'table',
  unique_key = 'atlas_nft_table_id',
  tags = ['atlas']
) }}

WITH nft_data AS (

  SELECT
    *
  FROM
    {{ ref('silver__atlas_nft_transactions') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(
    ['receiver_id']
  ) }} AS atlas_nft_table_id,
  receiver_id,
  COUNT(
    DISTINCT token_id
  ) AS tokens,
  COUNT(
    CASE
      WHEN method_name = 'nft_transfer'
      AND DAY >= (SYSDATE() :: DATE - INTERVAL '1 day') THEN tx_hash END
    ) AS transfers_24h,
    COUNT(
      CASE
        WHEN method_name = 'nft_transfer'
        AND DAY >= (SYSDATE() :: DATE - INTERVAL '3 day') THEN tx_hash END
      ) AS transfers_3d,
      COUNT(
        CASE
          WHEN method_name = 'nft_transfer' THEN tx_hash
        END
      ) AS all_transfers,
      COUNT(
        DISTINCT owner
      ) AS owners,
      COUNT(*) AS transactions,
      COUNT(
        CASE
          WHEN method_name != 'nft_transfer' THEN tx_hash
        END
      ) AS mints,
      SYSDATE() AS inserted_timestamp,
      SYSDATE() AS modified_timestamp,
      '{{ invocation_id }}' AS _invocation_id
      FROM
        nft_data
      GROUP BY
        1,
        2
      ORDER BY
        3 DESC
