{{ config(
    materialized = 'view',
    tags = ['s3', 's3_helper']
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
    WHERE
        _partition_by_block_number <= (
            SELECT
                MAX(_partition_by_block_number)
            FROM
                silver.streamline_receipts_final
        ) + 1000000
)
SELECT
    *
FROM
    receipts
