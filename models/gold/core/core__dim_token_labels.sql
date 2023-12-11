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
        decimals,
        token_labels_id AS dim_token_labels_id,
        COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__token_labels') }}
),
nearblocks_fts_api AS (
    {# Deprecated 9/25/2023, TODO update this view to new token ingestion job #}
    SELECT
        token,
        symbol,
        token_contract,
        decimals,
        COALESCE(
            api_nearblocks_fts_id,
            {{ dbt_utils.generate_surrogate_key(
                ['token_contract']
            ) }}
        ) AS dim_token_labels_id,
        COALESCE(inserted_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
        COALESCE(modified_timestamp,'2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
    FROM
        {{ ref('silver__api_nearblocks_fts') }}
),
FINAL AS (
    SELECT
        *
    FROM
        token_labels
    UNION ALL
    SELECT
        *
    FROM
        nearblocks_fts_api
)
SELECT
    *
FROM
    FINAL
