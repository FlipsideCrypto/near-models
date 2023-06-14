{{ config(
    materialized = 'view',
    tags = ['helper', 'receipt_map']
) }}

WITH receipts AS (

    SELECT
        A.receipt_id PARENT,
        b.value :: STRING item,
        block_id,
        _load_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__streamline_receipts') }} A
        JOIN LATERAL FLATTEN(
            A.outcome_receipts,
            outer => TRUE
        ) b

        {% if var("MANUAL_FIX" )  or var("MANUAL_FIX_DEV" )  %}
    WHERE
        {{ partition_load_manual('front') }}
    {% else %}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                silver.streamline_receipts_final
        ) - 20000
        AND _partition_by_block_number <= (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                silver.streamline_receipts_final
        ) + 220000
    {% endif %}
)
SELECT
    *
FROM
    receipts
