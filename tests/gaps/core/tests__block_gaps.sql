{{ config(
    severity = 'error',
    tags = ['gap_test']
) }}

WITH silver_blocks AS (

  SELECT
    block_id,
    LAG(block_id) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prior_block_id,
    block_id - prior_block_id AS gap_size,
    block_timestamp,
    block_hash,
    prev_hash,
    LAG(block_hash) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prior_hash,
    _partition_by_block_number,
    inserted_timestamp,
    SYSDATE() AS _test_timestamp
  FROM
    {{ ref('silver__blocks_final') }}

    {% if var('DBT_FULL_TEST') %}
    WHERE
      inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
    WHERE
      inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
      AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash 
