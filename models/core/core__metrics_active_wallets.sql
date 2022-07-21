{{ config(
    materialized = 'view'
) }}

WITH active_wallets AS (

    SELECT
        *
    FROM
        {{ ref('metrics__active_wallets') }}
)
SELECT
    DATE,
    daily_active_wallets,
    rolling_30day_active_wallets,
    rolling_7day_active_wallets
FROM
    active_wallets
