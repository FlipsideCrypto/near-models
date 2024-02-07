{{ config(
    error_if = '>=10',
    warn_if = 'BETWEEN 1 AND 9',
    tags = ['gap_test']
) }}

WITH shards AS (

    SELECT
        block_id,
        _partition_by_block_number,
        SUM(ARRAY_SIZE(chunk :transactions :: ARRAY)) AS tx_ct_expected
    FROM
        {{ ref('silver__streamline_shards') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
    GROUP BY
        1,
        2
),
txs AS (
    SELECT
        block_id,
        _partition_by_block_number,
        COUNT(DISTINCT tx_hash) AS tx_ct_actual_distinct,
        COUNT(1) AS tx_ct_actual_all
    FROM
        {{ ref('silver__streamline_transactions') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
    GROUP BY
        1,
        2
),
diffs AS (
    SELECT
        s.block_id,
        s.tx_ct_expected,
        t.tx_ct_actual_distinct,
        t.tx_ct_actual_all,
        s._partition_by_block_number
    FROM
        shards s
        LEFT JOIN txs t
        ON s.block_id = t.block_id
)
SELECT
    *
FROM
    diffs
WHERE
    tx_ct_expected != tx_ct_actual_distinct
    OR tx_ct_expected != tx_ct_actual_all
    OR tx_ct_actual_distinct != tx_ct_actual_all
ORDER BY
    block_id
