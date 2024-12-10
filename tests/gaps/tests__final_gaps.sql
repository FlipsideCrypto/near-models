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
        DISTINCT receipt_object_id,
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
),
r_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id
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

    -- AND
        -- _actions[0] is not null
),
f_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id
    FROM
        {{ ref('silver__streamline_transactions_final') }}

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
    'receipt_id' AS hash_type,
    r_receipts.receipt_id AS missing,
    r_receipts.block_id
FROM
    r_receipts
LEFT JOIN
    f_receipts
ON
    r_receipts.receipt_id = f_receipts.receipt_object_id
WHERE
    f_receipts.receipt_object_id IS NULL

UNION ALL

SELECT
    'tx_hash' AS hash_type,
    r_transactions.tx_hash AS missing,
    r_transactions.block_id
FROM
   r_transactions
LEFT JOIN
    f_transactions
ON
    r_transactions.tx_hash = f_transactions.tx_hash
WHERE
    f_transactions.tx_hash IS NULL
