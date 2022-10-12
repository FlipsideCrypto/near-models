{{ config(
    materialized = 'view',
    secure = true
) }}

with staking_pools as (
    select
        *
    from {{ ref('silver__staking_pools') }}
)

select * from staking_pools
