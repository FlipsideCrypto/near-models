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
    tx_id as txn_hash,
    block_timestamp,
    tx:signer_id::string as tx_signer,
    tx:receiver_id::string as tx_receiver,
    tx:actions[0] as actions_data,
    tx,  
    tx:outcome:outcome as outcome,
    tx:receipt[0] as receipt,
    case
      when value like '%CreateAccount%' then value
      else OBJECT_KEYS(value)[0]::string
    end as action_name,
    actions_data:Transfer.deposit::int as deposit,
    receipt:outcome:tokens_burnt::int + tx:outcome:outcome:tokens_burnt::int as tx_fee,
    receipt:outcome:gas_burnt::int + tx:outcome:outcome:gas_burnt::int as gas_used,
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
    txn_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    tx_fee,
    gas_used,
    status,
    try_parse_json(receipt) as receipt
  from actions

  )
  
select * from final