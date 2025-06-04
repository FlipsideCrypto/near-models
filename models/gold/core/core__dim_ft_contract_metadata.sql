{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    asset_identifier,
    source_chain,
    crosschain_token_contract,
    near_token_contract AS contract_address,
    NAME,
    symbol,
    decimals,
    ft_contract_metadata_id AS dim_ft_contract_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__ft_contract_metadata') }}
WHERE
    name is not null OR symbol is not null
