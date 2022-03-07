{{ 
     config(
         materialized = 'incremental',
         unique_key = 'tx_id',
         tags = ['core'],
         cluster_by = ['block_timestamp']
     ) 
}}
-- WIP -- below serves as example

with base_txs as (
    select * from {{ ref("stg_txs") }}
    where {{ incremental_load_filter("block_timestamp") }}
),
final as (  
    select
        *    
    from base_txs
)

select * from final
