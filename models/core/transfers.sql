{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='tx_id',
    tags=['core', 'transfers']
  )
}}

with action_events as(
  
select 
  txn_hash,
  action_data:deposit::int as deposit
  from {{ ref('action_events') }}
  where action_name = 'Transfer'

),
 
actions as (
  select
    t.txn_hash,
    t.block_timestamp,
    t.tx_receiver,
    t.tx_signer,
    a.deposit,
    t.transaction_fee,
    t.gas_used,
    t.tx_receipt[0]:id::string as receipt_id,
    case
        when tx_receipt[0]:outcome:status::string = '{"SuccessValue":""}' then 'Succeeded' 
        else 'Failed'
    end as status
  from {{ ref('transactions') }} as t
  inner join action_events as a on a.txn_hash = t.txn_hash

  ),

final as ( 

  select 
    txn_hash,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_id,
    tx_fee,
    gas_used,
    status
  from actions

  )
  
select * from final
