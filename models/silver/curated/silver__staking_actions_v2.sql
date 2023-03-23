{{config(materialized='table',
         unique_key='tx_hash',
         incremental_strategy='delete+insert',
         tags=['curated'],
         cluster_by=['_partition_by_block_number', 'block_timestamp::date']
         )
}}

with
-- actions as (
-- select * from near.silver.actions_events_s3
-- where tx_hash in (select distinct tx_hash from near_dev.silver.staking_diff_temp)
--     and block_timestamp::date <= '2023-03-22'
-- )
-- -- select count(distinct tx_hash) from actions;
-- ,
stake_actions as (
select * from near.silver.actions_events_s3
where lower(action_name) = 'stake' 
)
-- select count(distinct tx_hash) from stake_actions;
-- 24,404 vs 26,886, losing about 2400 txs (from diff) with no Stake action
,
flatten_logs as (
select 
    tx_hash,
    block_id,
    block_timestamp,
    receipt_object_id,
    receiver_id,
    signer_id,
    status_value,
    logs,
    value as log,
    _load_timestamp,
    _partition_by_block_number
from near.silver.streamline_receipts_final, lateral flatten(logs)
    where true
    and tx_hash in (select distinct tx_hash from stake_actions)
    and array_size(logs) > 0
),
staking_data as (
    select
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        status_value,
        log,
        split(log, ' ') as log_parts,
        split(log_parts[0]::string, '@')[1]::string as log_signer_id,
        log_parts[1]::string as log_action,
        split(log_parts[2]::string,'.')[0]::number as log_amount,
        log_amount / pow(10, 24) as log_amount_near,
        length(log_amount::string) as _log_amount_length,
        signer_id = log_signer_id as _log_signer_id_match,
        _load_timestamp,
        _partition_by_block_number
    from flatten_logs
    where true
        and receiver_id != signer_id
        and receiver_id ilike '%.pool%.near'
        and receiver_id not in ('usn-unofficial.poolowner.near', 'usn-unofficial.pool.near')
        and log like '@%'
        -- test against the below, shouldn't need to filter but idk
        -- and log_action in ('staking', 'unstaking', 'deposited')

        -- -- exclude epoch rewards, todo for separate model
        -- and log not like 'Epoch%'
        -- and log not ilike '%Record%reward from farm%'
        -- and log not like 'Contract total staked balance is%'
        -- and log not like 'Total rewards fee is%'
)
select
*
 from staking_data


    -- where tx_hash in (
    --     'GeiLBPrvF3MAbcPA74Nm1qNdXbsVhxYXhjdbDWGkyxwC', -- forg stake
    --     'C1uy4w3wqdBictXy4Jyiqj2yNZGEHzQxiEdgR79uy3wB', -- forg unstake
    --     '2Zqi2S7LDAUby3kpM2bYSLojX6TtTSLE7x5zTnSD5KKj', -- mdao stake
    --     'H2ozLWxhk6UbVAnTgzKFSKsuooi7kYsruJgfx2A1HEhy', -- random
    --     '5P7rw88ZBLzLsMZgfV5VsEXeLiXQ4KefVvq1puyBQJtU', -- arca unstake 1
    --     'ABcv2Aozny6Uvi5Lb8SC6YuVBbpY1uUTSS9fAQ2Phndy', -- arca stake 1
    --     '3smwZoSeWD5bzSScSnB3yRZJqVsjkuAZu33v78aqhUtj' -- dnd unstake 25
    -- )

