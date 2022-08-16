{{ config(
    materialized = 'view',
    secure = true
) }}

WITH oracle_prices AS (

    SELECT
        block_timestamp AS TIMESTAMP,
        token,
        symbol,
        token_contract,
        price_usd,
        source
    FROM
        {{ ref('silver__prices_oracle') }}
),
FINAL AS (
    SELECT
        *
    FROM
        oracle_prices
)
SELECT
    *
FROM
    FINAL
