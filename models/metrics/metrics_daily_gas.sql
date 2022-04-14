{{
    config(
        materialized='incremental',
        unique_key= 'date',
        incremental_strategy = 'delete+insert',
        tags=['metrics', 'transactions'],
        cluster_by = ['date']
    )
}}

with first as (

    select

        date_trunc('day', block_timestamp) as date,
        sum(gas_used) as daily_gas_used

    from {{ ref('transactions') }}
    where {{ incremental_last_x_days("date", 3) }}
    group by 1

),

second as (

    select

        date_trunc('day', block_timestamp) as date,
        round(avg(gas_price), 2) as avg_gas_price --units in yoctoNEAR (10^-24 NEAR)

    from {{ ref('blocks') }}
    where {{ incremental_last_x_days("date", 3) }}
    group by 1

  ),

final as (

    select
        f.date,
        f.daily_gas_used,
        s.avg_gas_price
    from first as f
    join second as s
        on f.date = s.date

)

select * from final
