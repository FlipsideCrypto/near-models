{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING, GOVERNANCE' }}}
) }}

WITH daily_balance AS (

    SELECT
        date_day AS DATE,
        address,
        balance,
        COALESCE(
            pool_balance_daily_id,
            {{ dbt_utils.generate_surrogate_key(
                ['date_day', 'address']
            ) }}
        ) AS fact_staking_pool_daily_balances_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__pool_balance_daily') }}
)
SELECT
    *
FROM
    daily_balance
