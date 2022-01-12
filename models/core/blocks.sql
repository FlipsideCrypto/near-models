{{
    config(
        materialized='incremental',
        unique_key='block_id',
        tags=['core'],
        cluster_by=['block_timestamp']
    )
}}

-- WIP -- below serves as example
with base_blocks as (

    select * from {{ deduped_blocks("near_blocks") }}

),
final as (
    select
        block_id,
        block_timestamp,
        header:hash::string as block_hash,
        header:parent_hash::string as block_parent_hash,
        header:miner::string as miner,
        header:size as size,
        -- Build out more columns here from `header`
        -- ...

    from base_blocks
    where {{ incremental_load_filter("block_timestamp") }}
)

select * from final
