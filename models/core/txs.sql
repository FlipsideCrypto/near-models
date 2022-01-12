{{ 
     config(
         materialized = 'incremental',
         unique_key = 'tx_hash',
         tags = ['core'],
         cluster_by = ['block_timestamp']
     ) 
}}

with base_txs as (

    select * from {{ deduped_txs("near_txs") }}

),

final as (
    
    select
    
        block_timestamp,
        tx:nonce::string as nonce,
        tx_block_index as index,
        tx:bech32_from::string as native_from_address,
        tx:bech32_to::string as native_to_address,
        tx:from::string as from_address,
        tx:to::string as to_address,
        tx:value as value,
        tx:block_number as block_number,
        tx:block_hash::string as block_hash,
        tx:gas_price as gas_price,
        tx:gas as gas,
        tx_id as tx_hash,
        tx:input::string as data,
        tx:receipt:status::string = '0x1'  as status
    
    from base_txs
where {{ incremental_load_filter("block_timestamp") }}
)

select * from final
