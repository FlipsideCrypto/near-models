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
    ) AS prior_hash
  FROM
    {{ ref('silver__blocks') }}
  WHERE
    block_timestamp < CURRENT_DATE - 1
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash
