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
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
gas_used AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        SUM(gas_used) AS daily_gas_used --gas units (10^-12 Tgas)
    FROM
        txs
    GROUP BY
        1
),
SECOND AS (
    SELECT
        DATE_TRUNC(
            'day',
            block_timestamp
        ) AS DATE,
        ROUND(AVG(gas_price), 2) AS avg_gas_price --units in yoctoNEAR (10^-24 NEAR)
    FROM
        blocks
    GROUP BY
        1),
        FINAL AS (
            SELECT
                f.date,
                f.daily_gas_used AS daily_gas_used,
                s.avg_gas_price AS avg_gas_price
            FROM
                gas_used AS f
                JOIN SECOND AS s
                ON f.date = s.date
        )
    SELECT
        *
    FROM
        FINAL
