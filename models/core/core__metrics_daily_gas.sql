{{ config(
    materialized = 'view',
    secure = true
) }}

WITH daily_gas AS (

    SELECT
        *
    FROM
        {{ ref('metrics__daily_gas') }}
)
SELECT
    avg_gas_price,
    daily_gas_used,
    DATE
FROM
    daily_gas
