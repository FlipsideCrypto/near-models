{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core']
) }}

WITH token_labels AS (

    SELECT
        token,
        symbol,
        token_contract,
        decimals
    FROM
        {{ ref('silver__token_labels') }}
),
nearblocks_fts_api AS (
{# Deprecated 9/25/2023, TODO update this view to new token ingestion job #}
    SELECT
        token,
        symbol,
        token_contract,
        decimals
    FROM
        {{ ref('silver__api_nearblocks_fts') }}
),
FINAL AS (
    SELECT
        *
    FROM
        token_labels
    UNION
    SELECT
        *
    FROM
        nearblocks_fts_api
)
SELECT
    DISTINCT *
FROM
    FINAL
