{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    tags=['near','actions','events','functioncall']
  )
}}

with
action_events as (

  select * from {{ ref('actions_events') }}
  where {{ incremental_load_filter('block_timestamp') }}
  and action_name = 'FunctionCall'

),

decoding as (
select

  *,
  try_base64_decode_string(action_data:args) as args_decoded,
  action_data:deposit::float / pow(10,24) as deposit,
  action_data:gas::float / pow(10,12) as attached_tgas,
  action_data:method_name::string as method_name

from action_events
where args_decoded is not null

),

function_calls as (
  select

    action_id,
    tx_hash,
    block_timestamp,
    action_name,
    method_name,
    try_parse_json(args_decoded) as args,
    deposit,
    attached_tgas

  from decoding
)

select * from function_calls
