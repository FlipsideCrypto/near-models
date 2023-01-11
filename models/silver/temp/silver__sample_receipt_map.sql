{{ config(
    materialized = 'table',
    enabled = False
) }}

{# TEMP table to ensure the test set is data we have and that the processing lag in blocks 
does not create funky results
TODO - delete before prod
 #}
WITH slice AS (

    SELECT
        *
    FROM
        {{ ref('silver__receipts') }}
    WHERE
        block_id between 81200000 and 82200000
),
sample_receipts AS (
    SELECT
        *
    FROM
        {{ ref('silver__sample_receipts') }}
    WHERE
        receipt_id IN (
            SELECT
                receipt_object_id
            FROM
                slice
        )
),
receipts AS (
    SELECT
        A.receipt_id PARENT,
        b.value :: STRING item,
        block_id
    FROM
        sample_receipts A
        JOIN LATERAL FLATTEN(
            A.outcome_receipts,
            outer => TRUE
        ) b
)
SELECT
    *
FROM
    receipts
