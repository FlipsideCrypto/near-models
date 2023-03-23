{{config(materialized='table')}}
;
with
staking_v1 as (
    select
    tx_hash,
    block_timestamp,
    pool_address,
    tx_signer,
    stake_amount / pow(10,24) as amount,
    action,
    'v1' as model
from near.silver.staking_actions_s3
),
staking_v2 as (
    select
    tx_hash,
    block_timestamp,
    receiver_id as pool_address,
    signer_id as tx_signer,
    log_amount_near as amount,
  iff(log_action in ('deposited', 'staking'), 'Stake', 'Unstake') as action,
  'v2' as model
    from near_dev.silver.staking_actions_v2
),
diff as (
    select distinct tx_hash from staking_v1
    except 
    select distinct tx_hash from staking_v2
)
select * from near.silver.streamline_receipts_final
where tx_hash in (select tx_hash from diff)
order by block_timestamp desc, tx_hash 
limit 100