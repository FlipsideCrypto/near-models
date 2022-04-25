{{
    config(
        materialized='table',
        tags=['metrics', 'transactions'],
        cluster_by = ['date']
    )
}}

with n_transactions as (

    select

        date_trunc('day', block_timestamp) as date,
        count(distinct txn_hash) as daily_transactions

    from {{ ref('transactions') }}

    group by 1
)

select * from n_transactions
