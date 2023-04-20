{{ config(
    materialized = 'table',
    cluster_by = ['date_day'],
    unique_key = '_id',
    tags = ['curated']
) }}

WITH pool_balances AS (

    SELECT
        *
    FROM
        {{ ref('silver__pool_balances') }}
),
all_staking_pools AS (
    SELECT
        receiver_id as address,
        min(block_timestamp::date) as min_date
    FROM
        {{ ref('silver__pool_balances') }}
    GROUP BY 1
),
dates AS (
    SELECT
        date_day
    FROM
        {{ source(
            'crosschain',
            'dim_dates'
        ) }}
    WHERE
        date_day >= '2020-08-25'
        AND date_day < CURRENT_DATE
),
boilerplate AS (
    SELECT
        ap.address,
        d.date_day
    FROM
        all_staking_pools ap
        CROSS JOIN dates d
    WHERE 
        d.date_day >= ap.min_date
),
daily_balance AS (
    SELECT
        block_timestamp :: DATE AS date_day,
        receiver_id as address,
        amount_adj as balance
    FROM
        pool_balances 
        qualify ROW_NUMBER() over (
            PARTITION BY address,
            block_timestamp :: DATE
            ORDER BY
                block_id DESC
        ) = 1
),
imputed_balance AS (
    SELECT
        b.date_day,
        b.address,
        daily.balance,
        LAG(balance) ignore nulls over (
            PARTITION BY address
            ORDER BY
                b.date_day
        ) AS imputed_bal_lag,
        COALESCE(
            balance,
            imputed_bal_lag
        ) AS daily_balance,
        LAST_VALUE( balance ignore nulls ) over( PARTITION BY address ORDER BY daily.date_day rows unbounded preceding ) AS daily_balance_2
    FROM
        boilerplate b
        LEFT JOIN daily_balance daily USING (
            date_day,
            address
        )
)
SELECT
    date_day,
    address,
    daily_balance as balance,
    CONCAT_WS('-', date_day, address) AS _id
FROM
    imputed_balance
