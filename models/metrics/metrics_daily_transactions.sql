{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    tags = ['metrics', 'transactions'],
    cluster_by = ['date']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('transactions') }}
    WHERE
        {{ incremental_last_x_days(
            "ingested_at",
            2
        ) }}
),
WITH n_transactions AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        COUNT(
            DISTINCT txn_hash
        ) AS daily_transactions
    FROM
        txs
    GROUP BY
        1
)
SELECT
    *
FROM
    n_transactions
