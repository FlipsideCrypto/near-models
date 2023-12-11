{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'PRICE' }}},
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
        source,
        COALESCE(
            prices_oracle_id,
            {{ dbt_utils.generate_surrogate_key(
                ['block_id', 'token_contract']
            ) }}
        ) AS fact_prices_id,
        COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__prices_oracle_s3') }}
)
SELECT
    *
FROM
    oracle_prices
