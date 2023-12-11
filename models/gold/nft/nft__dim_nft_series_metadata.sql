{{ config(
    materialized = 'view',
    tags = ['core', 'nft', 'pagoda']
) }}

WITH series_metadata AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_series_metadata') }}
)
SELECT
    contract_address,
    series_id,
    token_metadata :title :: STRING AS series_title,
    metadata_id,
    contract_metadata,
    token_metadata,
    COALESCE(
        nft_series_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['metadata_id']
        ) }}
    ) AS dim_nft_series_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    series_metadata
