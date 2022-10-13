{{ config(
    materialized = 'view',
    secure = true
) }}

with staking_actions as (
    select
        *
    from {{ ref('silver__staking_actions') }}
)

select * from staking_actions
