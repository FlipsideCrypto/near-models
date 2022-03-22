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

    block_id,
    tx_id,
    block_timestamp,
    tx,
    tx:outcome as tx_outcome,
    tx:receipt as tx_receipt,
    tx:receiver_id::string as tx_receiver,
    tx:signer_id::string as tx_signer

  from {{ ref('stg_txs') }}
  where {{ incremental_load_filter("block_timestamp") }}

)

select * from transactions
