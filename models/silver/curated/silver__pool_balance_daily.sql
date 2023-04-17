{{ config(
    materialized = 'incremental',
    cluster_by = ['_date'],
    unique_key = "CONCAT_WS('-', _date, address)",
    tags = ['curated']
) }}

WITH pool_balances AS (

    SELECT
        *
    FROM
        {{ ref('silver__pool_balances') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days('_load_timestamp', '1 day') }}
        {% endif %}
),
all_staking_pools AS (
    SELECT
        DISTINCT receiver_id as address
    FROM
        {{ ref('silver__pool_balances') }}
),
dates AS (
    SELECT
        date_day AS _date
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
        d._date
    FROM
        all_staking_pools ap
        CROSS JOIN dates d
),
daily_balance AS (
    SELECT
        block_timestamp :: DATE AS _date,
        receiver_id as address,
        amount_adj as balance,
        _load_timestamp
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
        b._date,
        b.address,
        daily.balance,
        LAG(balance) ignore nulls over (
            PARTITION BY address
            ORDER BY
                daily._date
        ) AS imputed_bal_lag,
        COALESCE(
            balance,
            imputed_bal_lag
        ) AS daily_balance,
        _load_timestamp
    FROM
        boilerplate b
        LEFT JOIN daily_balance daily USING (
            _date,
            address
        )
)
SELECT
    _date,
    address,
    daily_balance as balance,
    _load_timestamp
FROM
    imputed_balance
