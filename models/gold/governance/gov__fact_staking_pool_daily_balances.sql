{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING, GOVERNANCE'
            }
        }
    }
) }}

WITH daily_balance AS (

    SELECT
        date_day as date,
        address,
        balance
    FROM
        {{ ref('silver__pool_balance_daily') }}
)
SELECT
    *
FROM
    daily_balance
