{{ config(
    materialized = 'table',
    unique_key = 'receipt_id',
    cluster_by = ['_load_timestamp::date','block_id'],
    enabled = False

) }}

SELECT
    *
FROM
    {{ ref('silver__streamline_receipts') }}
WHERE
    block_id BETWEEN 81200000
    AND 82200000
