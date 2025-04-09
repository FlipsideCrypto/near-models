{{ config(
    severity = 'error',
    tags = ['gap_test', 'gap_test_core']
) }}
-- depends_on: {{ ref('silver__blocks_v2') }}

{% if execute %}

    {% if not var('DBT_FULL_TEST') %}
      {% set min_block_sql %}
        SELECT
          GREATEST(MIN(block_id), 142000000) AS block_id
        FROM
          {{ ref('silver__blocks_v2') }}
        WHERE
          block_timestamp >= SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
      {% endset %}
      {% set min_block_id = run_query(min_block_sql).columns[0].values()[0] %}
    {% else %}
      {% set min_block_id = 142000000 %}
    {% endif %}
  {% do log('Min block id: ' ~ min_block_id, info=True) %}
{% endif %}

WITH expected_chunks AS (

    SELECT
        block_id,
        _inserted_timestamp,
        VALUE :chunk_hash :: STRING AS chunk_hash,
        VALUE :height_created :: INT AS height_created,
        VALUE :height_included :: INT AS height_included
    FROM
        {{ ref('silver__blocks_v2') }}, lateral flatten(input => block_json :chunks :: ARRAY)
    WHERE
        block_id >= {{ min_block_id }}

    qualify(ROW_NUMBER() over (PARTITION BY chunk_hash ORDER BY block_id ASC)) = 1
),
actual_chunks AS (
    SELECT
        DISTINCT chunk_hash
    FROM
        {{ ref('silver__chunks_v2') }}
    WHERE
        block_id >= {{ min_block_id }}
)
SELECT
    block_id,
    _inserted_timestamp,
    chunk_hash,
    height_created,
    height_included
FROM
    expected_chunks e
    LEFT JOIN actual_chunks a USING (chunk_hash)
WHERE
    a.chunk_hash IS NULL
    AND _inserted_timestamp <= SYSDATE() - interval '1 hour'
    AND height_included >= {{ min_block_id }}
