{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    cluster_by = ['_load_timestamp::date','block_timestamp::date'],
    enabled = False

) }}

SELECT
    *
FROM
    {{ ref('silver__streamline_transactions') }}
WHERE
    block_id BETWEEN 81200000
    AND 82200000
