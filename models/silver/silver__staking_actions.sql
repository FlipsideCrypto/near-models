{{ config(
    materialized = 'incremental',
    incremental = 'merge',
    cluster_by = ['block_timestamp'],
    unique_key = 'tx_hash'
) }}

with actions_events_function_call as (
    select
        tx_hash,
        method_name,
        _inserted_timestamp
    from {{ ref('silver__actions_events_function_call') }}
    where {{ incremental_load_filter('_inserted_timestamp') }}
      and method_name in (
        'deposit_and_stake',
        'stake',
        'unstake',
        'unstake_all'
      )
),

base_txs as (
    select
        *
    from {{ ref('silver__transactions') }}
    where {{ incremental_load_filter('_inserted_timestamp') }}
),

txs as (
    select
        *
    from base_txs
    where (tx_receiver like '%.pool.near' or tx_receiver like '%.poolv1.near')
),

pool_txs as (
    select
        txs.tx_hash as tx_hash,
        block_timestamp,
        tx_receiver,
        tx_signer,
        tx,
        method_name,
        txs._inserted_timestamp as _inserted_timestamp
    from txs
    inner join actions_events_function_call
            on txs.tx_hash = actions_events_function_call.tx_hash
),

deposit_and_stake_txs as (
    select
        tx_hash,
        block_timestamp,
        tx_receiver as pool_address,
        tx_signer,
        regexp_substr(array_to_string(tx:receipt[0]:outcome:logs, ','), 'staking (\\d+)', 1, 1, 'e')::number as stake_amount,
        'Stake' as action,
        _inserted_timestamp
    from pool_txs
    where method_name = 'deposit_and_stake'
      and tx:receipt[0]:outcome:status:SuccessValue is not null
),

stake_txs as (
    select
        tx_hash,
        block_timestamp,
        tx_receiver as pool_address,
        tx_signer,
        regexp_substr(array_to_string(tx:receipt[0]:outcome:logs, ','), 'staking (\\d+)', 1, 1, 'e')::number as stake_amount,
        'Stake' as action,
        _inserted_timestamp
    from pool_txs
    where method_name = 'stake'
      and tx:receipt[0]:outcome:status:SuccessValue is not null
),

stake_all_txs as (
    select
        tx_hash,
        block_timestamp,
        tx_receiver as pool_address,
        tx_signer,
        regexp_substr(array_to_string(tx:receipt[0]:outcome:logs, ','), 'staking (\\d+)', 1, 1, 'e')::number as stake_amount,
        'Stake' as action,
        _inserted_timestamp
    from pool_txs
    where method_name = 'stake_all'
      and tx:receipt[0]:outcome:status:SuccessValue is not null
),

unstake_txs as (
    select
        tx_hash,
        block_timestamp,
        tx_receiver as pool_address,
        tx_signer,
        regexp_substr(array_to_string(tx:receipt[0]:outcome:logs, ','), 'unstaking (\\d+)', 1, 1, 'e')::number as stake_amount,
        'Unstake' as action,
        _inserted_timestamp
    from pool_txs
    where method_name = 'unstake'
      and tx:receipt[0]:outcome:status:SuccessValue is not null
),

unstake_all_txs as (
    select
        tx_hash,
        block_timestamp,
        tx_receiver as pool_address,
        tx_signer,
        regexp_substr(array_to_string(tx:receipt[0]:outcome:logs, ','), 'unstaking (\\d+)', 1, 1, 'e')::number as stake_amount,
        'Unstake' as action,
        _inserted_timestamp
    from pool_txs
    where method_name = 'unstake_all'
      and tx:receipt[0]:outcome:status:SuccessValue is not null
),

final as (
    select
        *
    from deposit_and_stake_txs
    union
    select
        *
    from stake_all_txs
    union
    select
        *
    from unstake_txs
    union
    select
        *
    from unstake_all_txs
)

select
*
from final
