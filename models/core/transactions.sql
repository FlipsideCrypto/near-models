{{ 
    config(
        materialized='incremental', 
        unique_key= 'txn_hash',
        incremental_strategy = 'delete+insert',
        tags=['core', 'transactions'],
        cluster_by = ['block_timestamp']
    ) 
}}

with transactions as (

  select 
    block_id as block_height,
    tx:outcome:block_hash::string as block_hash,
    tx_id as txn_hash,
    block_timestamp,
    tx:nonce::number as nonce,
    tx:signature::string as signature,
    tx:receiver_id::string as tx_receiver,
    tx:signer_id::string as tx_signer,
    tx,
    tx:outcome as tx_outcome,
    tx:receipt as tx_receipt,
    tx:outcome:outcome:gas_burnt::number as transaction_gas_burnt,
    tx:outcome:outcome:tokens_burnt::number as transaction_tokens_burnt,
    get(tx:actions, 0):FunctionCall:gas::number as attached_gas,
    ingested_at
  from {{ ref('stg_txs') }}
  where {{ incremental_load_filter("ingested_at") }}

),

receipts as (

  select 
    txn_hash,
    sum(value:outcome:gas_burnt::number) as receipt_gas_burnt,
    sum(value:outcome:tokens_burnt::number) as receipt_tokens_burnt
  from transactions, lateral flatten( input => tx_receipt )
  group by 1
),

final as (

  select 
    t.block_height,
    t.block_hash,
    t.txn_hash,
    t.block_timestamp,
    t.nonce,
    t.signature,
    t.tx_receiver,
    t.tx_signer,
    t.tx,
    t.tx_outcome,
    t.tx_receipt,
    t.transaction_gas_burnt + r.receipt_gas_burnt as gas_used,
    t.transaction_tokens_burnt + r.receipt_tokens_burnt as transaction_fee,
    coalesce(t.attached_gas, gas_used) as attached_gas,
    t.ingested_at
  from transactions as t 
  join receipts as r 
    on t.txn_hash = r.txn_hash

)

select * from final
