{{ config(
    materialized = 'view',
    secure = true,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'PRICE'
            }
        }
    },
    tags = ['core', 'price']
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
