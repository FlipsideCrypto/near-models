{{
    config(
        materialized='table',
        tags=['metrics'],
        cluster_by = ['date']
    )
}}

with active_wallets as (

    select

        date_trunc('day', block_timestamp) as date,
        count(distinct tx_signer) as daily_active_wallets,
        sum(daily_active_wallets) over (order by date rows between 6 preceding and current row) as rolling_7day_active_wallets,
        sum(daily_active_wallets) over (order by date rows between 29 preceding and current row) as rolling_30day_active_wallets

    from {{ ref('transactions') }}
    group by 1
)

select * from active_wallets
