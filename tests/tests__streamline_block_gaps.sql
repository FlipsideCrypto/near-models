{{ config(
  error_if = '>=10',
  warn_if = 'BETWEEN 1 AND 9'
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
    SYSDATE() AS _test_timestamp
  FROM
    {{ ref('silver__streamline_blocks') }}

    {% if var('DBT_FULL_TEST') %}
    WHERE
      _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
    {% else %}
    WHERE
      _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
      AND SYSDATE() - INTERVAL '1 hour'
    {% endif %}
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prior_hash <> prev_hash
  {# Filter out false positive from blocks at start of window (whose parent hash was cut off) #}
  AND (_inserted_timestamp > SYSDATE() - INTERVAL '7 days' + INTERVAL '1 hour')