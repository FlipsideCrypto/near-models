{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='action_id',
    tags=['core', 'transfers']
  )
}}

with action_events as(

  select 
    txn_hash,
    action_id,
    action_data:deposit::int as deposit
  from {{ ref('actions_events') }}
  where action_name = 'Transfer' and {{ incremental_load_filter("block_timestamp") }}

),
 
actions as (
 
  select
    t.txn_hash,
    a.action_id,
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
  where {{ incremental_load_filter("block_timestamp") }}

),

final as ( 

  select 
    txn_hash,
    action_id,
    block_timestamp,
    tx_signer,
    tx_receiver,
    deposit,
    receipt_id,
    transaction_fee,
    gas_used,
    status
  from actions

)
  
select * from final