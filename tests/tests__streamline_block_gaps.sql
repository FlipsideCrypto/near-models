{{ config(
  error_if = '>=25',
  warn_if = '<25'
) }}

WITH silver_blocks AS (

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
    _inserted_timestamp,
    CURRENT_TIMESTAMP AS _test_timestamp
  FROM
    {{ ref('silver__streamline_blocks') }}
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash
