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
  action_data:args as args,
  try_base64_decode_string(action_data:args) as args_decoded,
  action_data:deposit::number as deposit,
  action_data:gas::number as attached_gas,
  action_data:method_name::string as method_name

from action_events

),

function_calls as (
  select

    action_id,
    txn_hash,
    block_timestamp,
    action_name,
    method_name,
    case
      when args_decoded is null then args
      else try_parse_json(args_decoded)
    end as args,
    deposit,
    attached_gas

  from decoding
)

select * from function_calls
