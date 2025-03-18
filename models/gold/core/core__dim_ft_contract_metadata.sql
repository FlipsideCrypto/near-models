{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
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
    DATA,
    ft_contract_metadata_id AS dim_ft_contract_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    ft_contract_metadata
