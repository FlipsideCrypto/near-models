{{ config(
    materialized = 'view',
    tags = ['core', 'nft', 'livequery', 'nearblocks'],
    enabled = false
) }}

WITH nft_contract_metadata AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_contract_metadata') }}
)
SELECT
    contract_address,
    NAME,
    symbol,
    base_uri,
    icon,
    tokens,
    COALESCE(
        nft_contract_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }}
    ) AS dim_nft_contract_metadata_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    nft_contract_metadata
