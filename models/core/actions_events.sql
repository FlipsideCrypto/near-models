{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='action_id',
    tags=['actions']
  )
}}

with
txs as (

  select * from {{ ref('stg_txs') }}
  where {{ incremental_load_filter('block_timestamp') }}

),

actions as (

  select

    tx_id,
    block_timestamp,
    index as action_index,
    case
      when value like '%CreateAccount%' then value
      else OBJECT_KEYS(value)[0]::string
    end as action_name,
    case
      when action_name = 'CreateAccount' then '{}'
      else value[action_name]
    end as action_data

  from txs, lateral flatten( input => tx:actions )

),

final as (

  select

    concat_ws('-', tx_id, action_index) as action_id,
    tx_id as tx_hash,
    block_timestamp,
    action_index,
    action_name,
    try_parse_json(action_data) as action_data

  from actions

)

select * from final
