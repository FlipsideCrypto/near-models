{{ config(
    materialized = 'view'
) }}

WITH daily_transactions AS (

    SELECT
        *
    FROM
        {{ ref('metrics__daily_transactions') }}
)
SELECT
    daily_transactions,
    DATE
FROM
    daily_transactions
