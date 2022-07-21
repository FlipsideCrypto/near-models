{{ config(
    materialized = 'view',
    secure = true
) }}

WITH active_wallets AS (

    SELECT
        *
    FROM
        {{ ref('metrics__active_wallets') }}
)
SELECT
    daily_active_wallets,
    DATE,
    rolling_30day_active_wallets,
    rolling_7day_active_wallets
FROM
    active_wallets
