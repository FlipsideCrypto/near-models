{{ config(
    severity = 'error',
    tags = ['gap_test']
) }}

WITH shards AS (

    SELECT
        block_id,
        _partition_by_block_number,
        SUM(ARRAY_SIZE(receipt_execution_outcomes)) AS receipt_ct_expected
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
receipts AS (
    SELECT
        block_id,
        _partition_by_block_number,
        COUNT(DISTINCT receipt_id) AS receipt_ct_actual_distinct,
        COUNT(1) AS receipt_ct_actual_all
    FROM
        {{ ref('silver__streamline_receipts') }}

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
        s.receipt_ct_expected,
        r.receipt_ct_actual_distinct,
        r.receipt_ct_actual_all,
        s._partition_by_block_number
    FROM
        shards s
        LEFT JOIN receipts r
        ON s.block_id = r.block_id
)
SELECT
    *
FROM
    diffs
WHERE
    receipt_ct_expected != receipt_ct_actual_distinct
    OR receipt_ct_expected != receipt_ct_actual_all
    OR receipt_ct_actual_distinct != receipt_ct_actual_all
ORDER BY
    block_id
