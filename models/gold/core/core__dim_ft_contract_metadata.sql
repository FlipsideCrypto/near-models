{{ config(
    materialized = 'view',
    tags = ['core', 'livequery', 'nearblocks']
) }}

WITH ft_contract_metadata AS (

    SELECT
        *
    FROM
        {{ ref('silver__ft_contract_metadata') }}
)
SELECT
    contract_address,
    NAME,
    symbol,
    decimals,
    icon,
    COALESCE(
        ft_contract_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }}
    ) AS dim_ft_contract_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    ft_contract_metadata
