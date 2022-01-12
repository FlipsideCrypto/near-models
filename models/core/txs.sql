{{ 
     config(
         materialized = 'incremental',
         unique_key = 'tx_hash',
         tags = ['core'],
         cluster_by = ['block_timestamp']
     ) 
}}
-- WIP -- below serves as example

with base_txs as (
    select * from {{ deduped_txs("near_txs") }}
),
final as (  
    select
        block_timestamp,
        tx:nonce::string as nonce,
        tx_id as tx_hash,
        -- Build out more columns here from `txs`
        -- ...    
    from base_txs
where {{ incremental_load_filter("block_timestamp") }}
)

select * from final
