{{ config(
    materialized = 'view',
    tags = ['core', 'nft', 'livequery', 'nearblocks']
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
    tokens
FROM
    nft_contract_metadata
