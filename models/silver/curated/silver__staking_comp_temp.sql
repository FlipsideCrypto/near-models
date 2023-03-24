
with staked_v1 as (
select
  block_timestamp::date as date,
  action,
  count(distinct tx_hash) as txs,
  sum(stake_amount/pow(10,24)) as amount
from near.core.dim_staking_actions
group by 1, 2
),
staked_v2 as (
select
  block_timestamp::date as date,
  iff(log_action in ('deposited', 'staking'), 'Stake', 'Unstake') as action,
  count(distinct tx_hash) as txs,
  sum(log_amount_near) as amount
from near_dev.silver.staking_actions_v2
group by 1, 2
)
select
    v1.date as date_v1,
    v1.action as action_v1,
    v1.txs as txs_v1,
    v1.amount as amount_v1,
    v2.date as date_v2,
    v2.action as action_v2,
    v2.txs as txs_v2,
    v2.amount as amount_v2,
    coalesce(amount_v1,0) = coalesce(amount_v2,0) as amount_match
from staked_v1 v1
full join staked_v2 v2
on v1.date = v2.date and v1.action = v2.action
order by 1,2;

with staked as
(select
  block_timestamp::date as date,
  iff(log_action = 'staking','Stake','Unstake') as action, 
  sum(log_amount_near) as amount
from near_dev.silver.staking_actions_v2
where log_action in ('staking', 'unstaking')
group by 1, 2
)

select
  date,
  action,
  amount,
  sum(amount) over (partition by action order by date asc rows between unbounded preceding and current row) as staking_balance
from staked
order by 1,2;