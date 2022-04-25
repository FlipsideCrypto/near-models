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

final as (

    select
        *
    from {{ source("chainwalkers","near_blocks") }}
    where {{ incremental_load_filter("ingested_at") }}
    qualify row_number() over (partition by block_id order by ingested_at desc) = 1

)

select * from final
