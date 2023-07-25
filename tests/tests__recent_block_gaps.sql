{{ config(
  severity = 'warn',
  tags = ['recent_gap_test']
) }}

WITH recent_blocks AS (

  SELECT
    *
  FROM
    {{ ref('silver__streamline_blocks') }}
  WHERE
    block_timestamp :: DATE >= CURRENT_DATE - INTERVAL '2 days'
),
silver_blocks AS (
  SELECT
    block_id,
    block_id - 1 AS missing_block_id,
    block_timestamp,
    block_hash,
    prev_hash,
    LAG(block_hash) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prior_hash,
    _partition_by_block_number,
    CURRENT_TIMESTAMP AS _test_timestamp
  FROM
    recent_blocks
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash
