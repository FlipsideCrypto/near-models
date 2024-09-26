{{ config(
    severity = "error"
) }}


WITH r_receipts AS (
    SELECT
        DISTINCT receipt_id,
        block_id
    FROM
        {{ ref('silver__streamline_receipts') }}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days' AND SYSDATE() - INTERVAL '24 hour'
),
f_receipts AS (
    SELECT
        DISTINCT receipt_object_id,
        block_id
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days' AND SYSDATE() - INTERVAL '24 hour'
),
r_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id
    FROM
        {{ ref('silver__streamline_transactions') }}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days' AND SYSDATE() - INTERVAL '24 hour'
    AND
        _actions[0] is not null
),
f_transactions AS (
    SELECT
        DISTINCT tx_hash,
        block_id
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        _inserted_timestamp BETWEEN SYSDATE() - INTERVAL '7 days' AND SYSDATE() - INTERVAL '24 hour'
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
