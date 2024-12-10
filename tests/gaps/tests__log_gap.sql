{{ config(
    severity = "error"
) }}

WITH r_logs AS (

    SELECT
        receipt_object_id AS receipt_id,
        block_timestamp,
        block_id,
        ARRAY_SIZE(logs) AS log_ct
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        ARRAY_SIZE(logs) > 0 
        {% if var('DBT_FULL_TEST') %}
            AND _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
            AND _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
),
logs AS (
    SELECT
        receipt_object_id AS receipt_id,
        block_id,
        count(1) AS log_ct
    FROM
        {{ ref('silver__logs_s3') }}

        {% if var('DBT_FULL_TEST') %}
        WHERE
            _inserted_timestamp < SYSDATE() - INTERVAL '1 hour'
        {% else %}
        WHERE
            _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days'
            AND SYSDATE() - INTERVAL '1 hour'
        {% endif %}
    GROUP BY
        1
) 
SELECT
    r.receipt_id,
    r.block_id,
    r.block_timestamp,
    r.log_ct,
    l.log_ct,
    FLOOR(r.block_id, -3) AS _partition_by_block_number
FROM
    r_logs r
    LEFT JOIN logs l
        ON r.receipt_id = l.receipt_id
        AND r.block_id = l.block_id
WHERE 
    r.log_ct != l.log_ct
