{{ config(
    materialized = 'view'
) }}

WITH nearblocks_ft_api AS (

    SELECT
        DATE,
        symbol,
        token,
        token_contract,
        decimals,
        token_data,
        provider
    FROM
        {{ ref('silver__api_nearblocks_fts') }}
)
SELECT
    *
FROM
    nearblocks_ft_api
