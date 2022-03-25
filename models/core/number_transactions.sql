{{
    config(
        materialized='incremental',
        unique_key= 'date',
        incremental_strategy = 'delete+insert',
        tags=['core', 'transactions'],
        cluster_by = ['date']
    )
}}

with n_transactions as (

    select

        date_trunc('day', block_timestamp) as date,
        count(distinct txn_hash) as number_of_transactions

    from {{ ref('transactions') }}
    where {{ incremental_load_filter("date") }}

    group by 1
)

select * from n_transactions
order by date desc
