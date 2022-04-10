{{
    config(
        materialized='incremental',
        unique_key= 'date',
        incremental_strategy = 'delete+insert',
        tags=['metrics', 'transactions'],
        cluster_by = ['date']
    )
}}

with n_transactions as (

    select

        date_trunc('day', block_timestamp) as date,
        count(distinct txn_hash) as daily_transactions

    from {{ ref('transactions') }}
    where {{ incremental_last_x_days("date", 3) }}

    group by 1
)

select * from n_transactions
