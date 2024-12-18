{{ config(
    severity = "error",
    tags = ['gap_test']
) }}


WITH r_receipts AS (
    SELECT
        DISTINCT receipt_id,
        block_id
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
),
f_receipts AS (
    SELECT
        DISTINCT receipt_object_id AS receipt_id,
        block_id
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    
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
    r_receipts.receipt_id AS receipt_id,
    r_receipts.block_id
FROM
    r_receipts
LEFT JOIN
    f_receipts
ON
    r_receipts.receipt_id = f_receipts.receipt_id
WHERE
    f_receipts.receipt_id IS NULL
