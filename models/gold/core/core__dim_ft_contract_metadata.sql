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
    icon,
    COALESCE(
        ft_contract_metadata_id,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }}
    ) AS dim_ft_contract_metadata_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    ft_contract_metadata
