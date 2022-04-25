{{
  config(
    materialized='incremental',
    unique_key='action_id',
    cluster_by='block_timestamp',
    tags=['actions_events']
  )
}}

with
action_events as (

  select * from {{ ref('actions_events') }}
  where action_name = 'AddKey'
    and {{ incremental_load_filter('ingested_at') }}

),

addkey_events as (

  select

    action_id,
    txn_hash,
    block_timestamp,
    action_data:access_key:nonce::number as nonce,
    action_data:public_key::string as public_key,
    action_data:access_key:permission as permission,
    action_data:access_key:permission:FunctionCall:allowance::number as allowance,
    action_data:access_key:permission:FunctionCall:method_names::array as method_name,
    action_data:access_key:permission:FunctionCall:receiver_id::string as receiver_id,
    ingested_at

  from action_events

)

select * from addkey_events
