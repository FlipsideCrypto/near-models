{{ config(
  severity = 'error'
) }}

WITH silver_blocks AS (

  SELECT
    block_id,
    block_timestamp,
    block_hash,
    prev_hash,
    LAG(block_hash) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prior_hash,
    _partition_by_block_number
  FROM
    {{ ref('silver__streamline_blocks') }}
  WHERE
    block_timestamp::date < CURRENT_DATE
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash
