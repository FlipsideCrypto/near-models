{{ 
    config(
        materialized='incremental', 
        unique_key= 'tx_id',
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
    get(tx:receipt, 0):outcome:gas_burnt::number as receipt_gas_burnt,
    coalesce(get(tx:receipt, 1):outcome:gas_burnt::number, 0) as receipt_execution_gas_burnt,
    transaction_gas_burnt + receipt_gas_burnt + receipt_execution_gas_burnt as gas_used,
    coalesce(get(tx:actions, 0):FunctionCall:gas::number, gas_used) as attached_gas,
    tx:outcome:outcome:tokens_burnt::number as transaction_tokens_burnt,
    get(tx:receipt, 0):outcome:tokens_burnt::number as receipt_tokens_burnt,
    coalesce(get(tx:receipt, 1):outcome:tokens_burnt::number, 0) as receipt_execution_tokens_burnt,
    transaction_tokens_burnt + receipt_tokens_burnt + receipt_execution_tokens_burnt as transaction_fee
  from {{ ref('stg_txs') }}
  where {{ incremental_load_filter("block_timestamp") }}

),

final as (

  select 
    block_height,
    block_hash,
    txn_hash,
    block_timestamp,
    nonce,
    signature,
    tx_receiver,
    tx_signer,
    tx,
    tx_outcome,
    tx_receipt,
    gas_used,
    attached_gas,
    transaction_fee
  from transactions

)

select * from final
