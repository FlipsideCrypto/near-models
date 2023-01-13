{{ 
    config(
        materialized = 'incremental',
        cluster_by = ['block_timestamp'],
        unique_key = 'tx_hash',
        incremental_strategy = 'merge',
  tags = ['curated', 'curated_rpc']


    )
}}
with txs as (
    select
        tx_hash,
        block_timestamp,
        tx_signer,
        tx_receiver,
        tx,
        _inserted_timestamp
    from {{ ref('silver__transactions') }}
    where {{ incremental_load_filter('_inserted_timestamp') }}
),

function_calls as (
    select
        tx_hash,
        args,
        method_name,
        _inserted_timestamp
    from {{ ref('silver__actions_events_function_call') }}
    where method_name in ('create_staking_pool', 'update_reward_fee_fraction')
      and {{ incremental_load_filter('_inserted_timestamp') }}
),

pool_txs as (
    select
        txs.tx_hash as tx_hash,
        block_timestamp,
        tx_signer,
        tx_receiver,
        args,
        method_name,
        tx,
        txs._inserted_timestamp as _inserted_timestamp
    from txs
    inner join function_calls
            on txs.tx_hash = function_calls.tx_hash
    where tx:receipt[0]:outcome:status:SuccessValue is not null
      or (method_name = 'create_staking_pool'
          and tx:receipt[0]:outcome:status:SuccessReceiptId is not null
          and tx:receipt[1]:outcome:status:SuccessValue is not null)
),

final as (
    select 
        pool_txs.tx_hash as tx_hash,
        block_timestamp,
        iff(method_name = 'create_staking_pool', args::variant::object:owner_id, tx_signer) as owner,
        iff(method_name = 'create_staking_pool', tx:receipt[1]:outcome:executor_id::text, tx_receiver) as address,
        args::variant::object:reward_fee_fraction as reward_fee_fraction,
        iff(method_name = 'create_staking_pool', 'Create', 'Update') as tx_type,
        _inserted_timestamp
    from pool_txs
)

select * from final
