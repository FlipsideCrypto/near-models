{{
    config(
        materialized='incremental',
        unique_key= 'txn_hash',
        incremental_strategy = 'delete+insert',
        tags=['core', 'transactions'],
        cluster_by = ['block_timestamp']
    )
}}

select

    block_timestamp,
    block_hash,
    txn_hash,
    tx_receipt[0]:id::string as receipt_object_id,

    case when tx_receipt[0]:outcome:receipt_ids[1] is not NULL
         then tx_receipt[0]:outcome:receipt_ids
         else tx_receipt[0]:outcome:receipt_ids[0]::string
    end as receipt_outcome_id,

    tx_receipt[0]:outcome:status as status_value,
    tx_receipt[0]:outcome:logs as logs,
    tx_receipt[0]:proof as proof,
    tx_receipt[0]:outcome:metadata as metadata

from {{ ref('transactions') }}
order by block_timestamp desc
