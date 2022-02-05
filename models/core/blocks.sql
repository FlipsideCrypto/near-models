{{
    config(
        materialized='incremental',
        unique_key='block_id',
        incremental_strategy = 'delete+insert',
        tags=['core'],
        cluster_by=['block_timestamp']
    )
}}

with

base_blocks as (

    select
        *
    from {{ ref("stg_blocks") }}
    where {{ incremental_load_filter("block_timestamp") }}

),
final as (

    select
        header:height::integer as block_id,
        div0(header:timestamp::integer,1000000000)::timestamp as block_timestamp,
        header:hash::string as block_hash,
        header:tx_count::integer as tx_count,
        header:author::string as block_author,
        header:challenges_result as block_challenges_result,
        header:challenges_root::string as block_challenges_root,
        header:chunk_headers_root::string as chunk_headers_root,
        header:chunk_mask as chunk_mask,
        header:chunk_receipts_root::string as chunk_receipts_root,
        header:chunk_tx_root::string as chunk_tx_root,
        header:chunks as chunks,
        header:chunks_included::integer as chunks_included,
        header:epoch_id::string as epoch_id,
        header:epoch_sync_data_hash::string as epoch_sync_data_hash,
        header:events as events,
        header:gas_price::float as gas_price,
        header:last_ds_final_block::string as last_ds_final_block,
        header:last_final_block::string as last_final_block,
        header:latest_protocol_version::integer as latest_protocol_version,
        header:next_bp_hash::string as next_bp_hash,
        header:next_epoch_id::string as next_epoch_id,
        header:outcome_root::string as outcome_root,
        header:prev_hash::string as prev_hash,
        header:prev_height::integer as prev_height,
        header:prev_state_root::string as prev_state_root,
        header:random_value::string as random_value,
        header:rent_paid::float as rent_paid,
        header:signature::string as signature,
        header:total_supply::float as total_supply,
        header:validator_proposals as validator_proposals,
        header:validator_reward::float as validator_reward
    from base_blocks

)

select * from final
