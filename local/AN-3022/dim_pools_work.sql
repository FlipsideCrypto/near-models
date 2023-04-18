select * from near_dev.core.fact_staking_pool_daily_balances
order by date desc, balance desc 
limit 1000;

select * from near_dev.silver.pool_balance_daily
order by date_day asc, balance desc
limit 100;
where not is_equal;

with
balances as (
    select distinct address from near_dev.silver.pool_balance_daily
),
metadata as (
    select distinct address from near.silver.staking_pools_s3
    where tx_type = 'Create'
)
select * from balances
except
select * from metadata;


select distinct address from near_dev.silver.pool_balance_daily
where address ilike '%nearcrowd%';

select * from near.silver.staking_pools_s3
-- where address = 'nearcrowd.poolv1.near';
where address ilike '%nearcrowd%';

with pool_transactions as (
    select distinct tx_hash from near.silver.streamline_receipts_final
    where receiver_id in ('poolv1.near', 'pool.near')
        or receiver_id ilike 'poolv%.near'
),
decoded_actions as (
    select * from near.silver.actions_events_function_call_s3
    where tx_hash in (
        select tx_hash from pool_transactions
        )
)
select
    tx_hash,
    action_id,
    split(action_id, '-')[0]::string as rec_id,
    block_id,
    args
from decoded_actions 
where method_name = 'create_staking_pool'
order by block_id
limit 100;

    select 
    receiver_id,

    count(distinct tx_hash)
    from near.silver.streamline_receipts_final
    where receiver_id in ('poolv1.near', 'pool.near')
        or receiver_id ilike 'poolv%.near'
        group by 1