{{ config(
    materialized = 'view',
    secure = true,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'STAKING'
            }
        }
    },
    tags = ['core']
) }}

WITH daily_balance AS (

    SELECT
        _date as date,
        address,
        balance
    FROM
        {{ ref('silver__pool_balance_daily') }}
)
SELECT
    *
FROM
    daily_balance
