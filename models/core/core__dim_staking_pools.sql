{{ config(
    materialized = 'view',
    secure = true
) }}

WITH staking_pools AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_pools') }}
)
SELECT
    tx_hash,
    block_timestamp,
    owner,
    address,
    reward_fee_fraction,
    tx_type
FROM
    staking_pools
