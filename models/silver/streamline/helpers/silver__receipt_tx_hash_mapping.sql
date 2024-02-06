{{ config(
    materalized = 'view',
    unique_key = 'receipt_id',
    tags = ['helper', 'receipt_map']
) }}

WITH 
recursive ancestrytree AS (

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
        tx_hash,
        outcome_receipts,
        _partition_by_block_number
    FROM
        {{ ref('silver__streamline_transactions') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('front') }}
        {% else %}
        WHERE
            _partition_by_block_number >= (
                SELECT
                    MIN(_partition_by_block_number) - (3000 * {{ var('RECEIPT_MAP_LOOKBACK_HOURS') }})
                FROM
                    {{ ref('_retry_range')}}
            )
        {% endif %}
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
