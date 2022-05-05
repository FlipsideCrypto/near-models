{{
    config(
        materialized='incremental',
        unique_key= 'date',
        incremental_strategy = 'delete+insert',
        tags=['metrics'],
        cluster_by = ['date']
    )
}}

with active_wallets as (

    select

        date_trunc('day', block_timestamp) as date,
        count(distinct tx_signer) as daily_active_wallets,
        sum(daily_active_wallets) over (order by date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_active_wallets,
        sum(daily_active_wallets) over (order by date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as monthly_active_wallets

    from {{ ref('transactions') }}
    where {{ incremental_last_x_days("date", 7) }}
    group by 1
)

select * from active_wallets
