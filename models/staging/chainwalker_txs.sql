{{
    config(
        materialized='incremental',
        unique_key='tx_id',
        incremental_strategy = 'delete+insert',
        tags=['core', 'transactions'],
        cluster_by=['block_timestamp']
        )
}}

with

final as (

    select
        *
    from {{ source("chainwalkers","near_txs") }}
    where {{ incremental_load_filter("block_timestamp") }}
    qualify row_number() over (partition by block_id order by ingested_at desc) = 1

)

select * from final
