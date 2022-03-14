{{
  config(
    materialized='table',
    unique_key='action_id'
  )
}}

with
action_events as (

  select * from {{ ref('actions_events') }}
  where action_name = 'AddKey'

),

addkey_events as (

  select

    action_id,
    tx_hash,
    block_timestamp,
    action_data,
    action_data:access_key:nonce::float as nonce,
    action_data:public_key::string as public_key,
    action_data:access_key:permission as permission,
    action_data:access_key:permission:FunctionCall:allowance::float as allowance,
    action_data:access_key:permission:FunctionCall:method_names as method_name,
    action_data:access_key:permission:FunctionCall:receiver_id::string as receiver_id

  from action_events

)

select * from addkey_events
