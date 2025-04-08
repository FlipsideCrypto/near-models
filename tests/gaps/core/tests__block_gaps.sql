{{ config(
    severity = 'error',
    tags = ['gap_test', 'gap_test_core']
) }}
-- depends_on: {{ ref('silver__blocks_v2') }}

{% if execute %}

    {% if not var('DBT_FULL_TEST') %}
      {% set min_block_sql %}
        SELECT
          GREATEST(MIN(block_id), 140868759) AS block_id
        FROM
          {{ ref('silver__blocks_v2') }}
        WHERE
          _inserted_timestamp >= SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
      {% endset %}
      {% set min_block_id = run_query(min_block_sql).columns[0].values()[0] %}
    {% else %}
      {% set min_block_id = 9820210 %}
    {% endif %}
  {% do log('Min block id: ' ~ min_block_id, info=True) %}
{% endif %}


WITH silver_blocks AS (

  SELECT
    block_timestamp,
    block_hash,
    block_id,
    LAG(block_id) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prior_block_id,
    block_id - prior_block_id AS gap_size,
    prev_hash,
    LAG(block_hash) over (
      ORDER BY
        block_timestamp ASC,
        block_id ASC
    ) AS prev_hash_actual
  FROM
    {{ ref('silver__blocks_final') }}


  WHERE
    block_id >= {{ min_block_id }}
  
)
SELECT
  *
FROM
  silver_blocks
WHERE
  prev_hash != COALESCE(prev_hash_actual, '')
  AND block_id != {{ min_block_id }}
