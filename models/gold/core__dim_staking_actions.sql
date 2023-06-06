{{ config(
    materialized = 'view',
    secure = true,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'STAKING'
            }
        }
    },
    tags = ['core']
) }}

with staking_actions as (
    select
        *
    from {{ ref('silver__staking_actions_s3') }}
)

select 
    tx_hash,
    block_timestamp,
    pool_address,
    tx_signer,
    stake_amount,
    action
 from staking_actions
