{{ config(
    materialized = 'view',
    secure = false,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'PRICES'
            }
        }
    },
    tags = ['core', 'prices']
) }}

WITH oracle_prices AS (

    SELECT
        block_timestamp AS TIMESTAMP,
        token,
        symbol,
        token_contract,
        raw_price,
        price_usd,
        source
    FROM
        {{ ref('silver__prices_oracle_s3') }}
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
