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
    icon
FROM
    ft_contract_metadata
