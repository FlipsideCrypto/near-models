{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='tx_id',
    tags=['transfers']
  )
}}

with
txs as (

  select 
    * 
  from {{ ref('stg_txs') }}
  where {{ incremental_load_filter('block_timestamp') }} 

),
 
actions as (

  select
    tx_id,
    block_timestamp,
    tx:signer_id::string as signer_id,
    tx:receiver_id::string as reciever_id,
    tx:actions[0] as actions_data,
    tx,  
    tx:outcome:outcome as outcome,
    tx:receipt[0] as receipt,
    case
      when value like '%CreateAccount%' then value
      else OBJECT_KEYS(value)[0]::string
    end as action_name,
    actions_data:Transfer.deposit::int / pow(10,24) as deposit,
    receipt:outcome:tokens_burnt::int / pow(10,24) + tx:outcome:outcome:tokens_burnt::int / pow(10,24) as tx_fee,
    receipt:outcome:gas_burnt::int/ pow(10,12) + tx:outcome:outcome:gas_burnt::int / pow(10,12) as Tgas_used,
    case
        when receipt:outcome:status::string = '{"SuccessValue":""}' then 'Succeeded' 
        else 'Failed'
    end as status
  from txs, 
  lateral flatten( input => tx:actions )
  where action_name = 'Transfer'

  ),

final as ( 

  select 
    tx_id,
    block_timestamp,
    signer_id,
    reciever_id,
    deposit,
    tx_fee,
    Tgas_used,
    status,
    try_parse_json(receipt) as receipt
  from actions

  )
  
select * from final