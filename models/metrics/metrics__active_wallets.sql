{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    tags = ['metrics'],
    cluster_by = ['date']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
active_wallets AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        COUNT(
            DISTINCT tx_signer
        ) AS daily_active_wallets,
        SUM(daily_active_wallets) over (
            ORDER BY
                DATE rows BETWEEN 6 preceding
                AND CURRENT ROW
        ) AS rolling_7day_active_wallets,
        SUM(daily_active_wallets) over (
            ORDER BY
                DATE rows BETWEEN 29 preceding
                AND CURRENT ROW
        ) AS rolling_30day_active_wallets
    FROM
        txs
    GROUP BY
        1
)
SELECT
    *
FROM
    active_wallets
