{{ config(
    materalized = 'view',
    unique_key = 'receipt_id'
) }}

WITH recursive ancestrytree AS (

    SELECT
        item,
        PARENT
    FROM
        {{ ref('silver__flatten_receipts') }}
    WHERE
        PARENT IS NOT NULL
    UNION ALL
    SELECT
        items.item,
        t.parent
    FROM
        ancestrytree t
        JOIN {{ ref('silver__flatten_receipts') }}
        items
        ON t.item = items.parent
),
FINAL AS (
    SELECT
        tx_hash,
        A.item,
        FALSE is_primary_receipt
    FROM
        ancestrytree A
        JOIN {{ ref('silver__streamline_transactions') }}
        b
        ON A.parent = b.outcome_receipts [0] :: STRING
    WHERE
        item IS NOT NULL
    UNION ALL
    SELECT
        A.tx_hash,
        outcome_receipts [0] :: STRING AS receipt_id,
        TRUE is_primary_receipt
    FROM
        {{ ref('silver__streamline_transactions') }} A
)
SELECT
    tx_hash,
    item AS receipt_id,
    is_primary_receipt
FROM
    FINAL
