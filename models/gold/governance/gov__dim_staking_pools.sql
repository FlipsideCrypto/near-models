{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING, GOVERNANCE' }}}
) }}

WITH staking_pools AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_pools_s3') }}
)
SELECT
    tx_hash,
    block_timestamp,
    owner,
    address,
    reward_fee_fraction,
    tx_type,
    COALESCE(
        staking_pools_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS dim_staking_pools_id,
    COALESCE(inserted_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS inserted_timestamp,
    COALESCE(modified_timestamp, _inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) AS modified_timestamp
FROM
    staking_pools
