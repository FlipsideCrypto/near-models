{{ config(
    materialized = 'view',
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'DEFI, TOKENS'
            }
        }
    },
    tags = ['core']
) }}
{# Deprecated 9/25/2023 TODO deprecate and delete in favor of new view #}

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
