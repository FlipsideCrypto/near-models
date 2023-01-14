{{ config(
    materalized = 'view',
    unique_key = 'receipt_id',
    tags = ['s3', 's3_helper']
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
txs AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions') }}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                silver.streamline_receipts_final
        ) - 10000
        AND _partition_by_block_number <= (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                silver.streamline_receipts_final
        ) + 1000000
),
FINAL AS (
    SELECT
        tx_hash,
        A.item,
        FALSE is_primary_receipt
    FROM
        ancestrytree A
        JOIN txs b
        ON A.parent = b.outcome_receipts [0] :: STRING
    WHERE
        item IS NOT NULL
    UNION ALL
    SELECT
        A.tx_hash,
        outcome_receipts [0] :: STRING AS receipt_id,
        TRUE is_primary_receipt
    FROM
        txs A
)
SELECT
    tx_hash,
    item AS receipt_id,
    is_primary_receipt
FROM
    FINAL
