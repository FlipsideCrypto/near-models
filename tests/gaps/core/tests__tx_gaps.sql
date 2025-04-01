{{ config(
    severity = 'error',
    tags = ['gap_test', 'gap_test_core']
) }}
-- depends_on: {{ ref('silver__blocks_v2') }}

{% if execute %}

    {% if not var('DBT_FULL_TEST') %}
      {% set min_block_sql %}
        SELECT
          MIN(block_id)
        FROM
          {{ ref('silver__blocks_v2') }}
        WHERE
          _inserted_timestamp >= SYSDATE() - INTERVAL '{{ var('DBT_TEST_LOOKBACK_DAYS', 14) }} days'
      {% endset %}
      {% set min_block_id = run_query(min_block_sql).columns[0].values()[0] %}
    {% else %}
      {% set min_block_id = 140868759 %}
    {% endif %}
  {% do log('Min block id: ' ~ min_block_id, info=True) %}
{% endif %}

WITH expected_txs AS (

    SELECT
        block_id,
        chunk_hash,
        chunk_json :height_created :: INT as chunk_height_created,
        chunk_json :height_included :: INT as chunk_height_included,
        _inserted_timestamp,
        VALUE :hash :: STRING AS tx_hash
    FROM
        {{ ref('silver__chunks_v2') }}, lateral flatten(input => chunk_json :transactions :: ARRAY)
    WHERE
        block_id >= {{ min_block_id }}
),
actual_txs AS (
    SELECT
        DISTINCT tx_hash
    FROM
        {{ ref('silver__transactions_v2') }}
    WHERE
        origin_block_id >= {{ min_block_id }}
)
SELECT
    block_id,
    chunk_hash,
    chunk_height_created,
    chunk_height_included,
    _inserted_timestamp,
    tx_hash
FROM
    expected_txs e
    LEFT JOIN actual_txs a USING (tx_hash)
WHERE
    a.tx_hash IS NULL
    AND _inserted_timestamp <= SYSDATE() - interval '2 hours'
    AND chunk_height_included >= {{ min_block_id }}
