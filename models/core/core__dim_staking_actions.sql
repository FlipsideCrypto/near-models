{{ config(
    materialized = 'view',
    secure = true,
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'STAKING'
            }
        }
    }
) }}

with staking_actions as (
    select
        *
    from {{ ref('silver__staking_actions') }}
)

select * from staking_actions
