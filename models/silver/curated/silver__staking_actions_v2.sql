{{config(materialized='table',
         unique_key='tx_hash',
         incremental_strategy='delete+insert',
         tags=['curated'],
         cluster_by=['_partition_by_block_number', 'block_timestamp::date']
         )
}}
with
stake_actions as (
select * from near.silver.actions_events_s3
where lower(action_name) = 'stake' 
),
staking as (
select 
    tx_hash,
    block_id,
    block_timestamp,
    receipt_object_id,
    receiver_id as staking_pool,
    signer_id,
    status_value,
    logs,
    split(logs[0], ' ') as log_parts,
    split(log_parts[0]::string, '@')[1]::string as log_signer_id,
    log_parts[1]::string as log_action,
    split(log_parts[2]::string,'.')[0]::number as log_amount,
    log_amount / pow(10, 24) as log_amount_near,
    length(split(log_parts[2]::string,'.')[0]::string) as _log_amount_length,
    signer_id = log_signer_id as _log_signer_id_match,
    _load_timestamp,
    _partition_by_block_number
from near.silver.streamline_receipts_final
    where true
    and tx_hash in (select distinct tx_hash from stake_actions)
    and receiver_id != signer_id
    and receiver_id ilike '%.pool%.near'
    and receiver_id not in ('usn-unofficial.poolowner.near', 'usn-unofficial.pool.near')
    and array_size(logs) > 0
    -- exclude epoch rewards, todo for separate model
    and logs[0] not like 'Epoch%'
    and logs[0] not ilike '%Record%reward from farm%'
    -- explicit:
    and log_action in ('staking', 'unstaking', 'deposited')

)
select * from staking
-- where block_timestamp::date >= current_date - interval '7 days'
-- limit 500;
    -- where tx_hash in (
    --     'GeiLBPrvF3MAbcPA74Nm1qNdXbsVhxYXhjdbDWGkyxwC', -- forg stake
    --     'C1uy4w3wqdBictXy4Jyiqj2yNZGEHzQxiEdgR79uy3wB', -- forg unstake
    --     '2Zqi2S7LDAUby3kpM2bYSLojX6TtTSLE7x5zTnSD5KKj', -- mdao stake
    --     'H2ozLWxhk6UbVAnTgzKFSKsuooi7kYsruJgfx2A1HEhy', -- random
    --     '5P7rw88ZBLzLsMZgfV5VsEXeLiXQ4KefVvq1puyBQJtU', -- arca unstake 1
    --     'ABcv2Aozny6Uvi5Lb8SC6YuVBbpY1uUTSS9fAQ2Phndy', -- arca stake 1
    --     '3smwZoSeWD5bzSScSnB3yRZJqVsjkuAZu33v78aqhUtj' -- dnd unstake 25
    -- )

