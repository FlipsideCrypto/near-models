{{ config(
    materialized = 'view',
    secure = false,
    tags = ['core', 'governance'],
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING, GOVERNANCE'
            }
        }
    }
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
    tx_type
FROM
    staking_pools
