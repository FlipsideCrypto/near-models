select * from near.silver.streamline_receipts_final
where block_id >= 110575625
and tx_hash in
(
    'GP9Q2T5Y5b6RgFHuWjYTjpYdzMLb6L8fYW9mDHWcjGMH', -- neko to wNEAR
    'Aix5SgyxvrR64oXCVo3iQuEWu8iqUXPyoXUVuzKhn2RD', -- neko to NEAR
    '7kZP9UPYFbGFC9zBhyrUtit1AL4KtMWYtrG62LJnDexY', -- NEAR to REF
    'GHS42kw1eVBARdcQWXEz4fhGMS7vuSyrqgYnBZ8kJULC' -- REF to NEAR
)
order by tx_hash, block_id;

select * from near.silver.streamline_receipts_final
where block_id >= 107000000
and tx_hash = 'AGkmKnRkVEdN2kF79HFeX41wso5jkgJHnxNNXfundde1'
order by tx_hash, block_id;

select distinct receiver_id from near.silver.streamline_receipts_final
where receiver_id ilike '%ref-finance.near';
-- 1token.v2.ref-finance.near
-- v2ref-finance.near
-- token.v2.ref-finance.near
-- v2.ref-finance.near
-- ref-finance.near
-- token.ref-finance.near
-- tokenv2ref-finance.near
-- xtoken.ref-finance.near

select * from near.silver.streamline_receipts_final
where receiver_id = 'ref-finance.near'
limit 15;

select 
    receiver_id,
    count(1),
    any_value(clean_log)
from near.silver.logs_s3
where clean_log like 'Swap%'
group by 1;

with
swap_receiver_ids as (
    select *from near.silver.logs_s3
    where clean_log like 'Swapped%'
),
actions as (
    select * from near.silver.actions_events_function_call_s3
    where split(action_id, '-')[0]::STRING in (select * from swap_receiver_ids)
)
select * from actions limit 5;

-- the inputs are wildly different, based on each contract
-- but they all follow basically the same log output...


with
swap_logs as (
    select * from near.silver.logs_s3
    where clean_log like 'Swapped%'
)
select
    tx_hash,
    receipt_object_id,
    block_id,
    block_timestamp,
    receiver_id,
    signer_id,
    clean_log as log,
  REGEXP_REPLACE(log, '.*Swapped (\\d+) (.*) for (\\d+) (.*)', '\\1')::int AS amount_out_raw,
  REGEXP_REPLACE(log, '.*Swapped \\d+ (\\S+) for (\\d+) (.*)', '\\1')::string AS token_out,
  REGEXP_REPLACE(log, '.*Swapped \\d+ \\S+ for (\\d+) (.*)', '\\1')::int AS amount_in_raw,
  REGEXP_REPLACE(log, '.*Swapped \\d+ \\S+ for \\d+ (.*)', '\\1')::string AS token_in,
    coalesce(_inserted_timestamp, _load_timestamp) as _inserted_timestamp,
    _partition_by_block_number,
    logs_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id

from swap_logs 
where log ilike '%with admin fee%'
limit 15;



-- mapping just ref
with 
ref_finance as (
    select *,
        array_size(receipt_actions:receipt:Action:actions::array) as action_ct    
     from near.silver.streamline_receipts_final
    where receiver_id in ('ref-finance.near', 'v2.ref-finance.near')
    and block_timestamp >= current_date - interval '7 days'
    and receipt_succeeded
),
flatten_actions as (
select 
    tx_hash,
    block_id,
    block_timestamp,
    receipt_object_id,
    receiver_id,
    signer_id,
    action_ct,
    INDEX as action_index,
    logs,
    -- VALUE,
    VALUE:FunctionCall:method_name::string as method_name,
    try_parse_json(try_base64_decode_string(VALUE:FunctionCall:args)) as args,
    receipt_succeeded
 from ref_finance, lateral flatten (receipt_actions:receipt:Action:actions)
 where true
--  and method_name = 'swap'
--  and tx_hash = '5RBZcAPHgE87qBqMZosNYQzTy3zUMSmHDpXywdFUbuEh'
),
flatten_function_call as (
select 
    tx_hash,
    block_id,
    block_timestamp,
    receipt_object_id,
    receiver_id,
    signer_id,
    action_ct,
    action_index,
    logs,
    method_name,
    VALUE,
    VALUE:amount_in::int as amount_in,
    VALUE:min_amount_out::int as min_amount_out,
    VALUE:token_in::string as token_in,
    VALUE:token_out::string as token_out,
    VALUE:pool_id::string as pool_id, -- TODO check if always int then change dtype
    INDEX as swap_index
 from flatten_actions, lateral flatten(args:actions)
)
select * from flatten_function_call
limit 50;

select * from near_dev.silver.ref_swaps
where tx_hash = 'AfvgkUxP8taJNBLaZYvFumFrrePpJujb2gjQJz7YbRiM'
limit 50;

-- {
--   "actions": [
--     {
--       "amount_in": "1228685760498046875000000",
--       "min_amount_out": "0",
--       "pool_id": 4,
--       "token_in": "wrap.near",
--       "token_out": "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near"
--     },
--     {
--       "min_amount_out": "0",
--       "pool_id": 1910,
--       "token_in": "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near",
--       "token_out": "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near"
--     },
--     {
--       "min_amount_out": "0",
--       "pool_id": 3024,
--       "token_in": "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near",
--       "token_out": "marmaj.tkn.near"
--     },
--     {
--       "min_amount_out": "0",
--       "pool_id": 3042,
--       "token_in": "marmaj.tkn.near",
--       "token_out": "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near"
--     },
--     {
--       "min_amount_out": "1228685760498046875000000",
--       "pool_id": 1395,
--       "token_in": "aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near",
--       "token_out": "wrap.near"
--     }
--   ]
-- }


-- vs logs below. So, the input data does not have the intermediate token amounts... 
-- nearblocks must be parsing the log, then. Or retrieving the amounts from elsewhere, but likely logs


-- Swapped 1228685760498046875000000 wrap.near for 1370018 dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near
-- Exchange v2.ref-finance.near got 4287440486291960659 shares, No referral fee
-- Swapped 1370018 dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near for 1369532 a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near, total fee 685, admin fee 137
-- Exchange v2.ref-finance.near got 135820392861051 shares, No referral fee
-- Swapped 1369532 a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near for 1135700395033097621 marmaj.tkn.near
-- Exchange v2.ref-finance.near got 77847931402681928990 shares, No referral fee
-- Swapped 1135700395033097621 marmaj.tkn.near for 26433575129761361463 aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near
-- Exchange v2.ref-finance.near got 999982423502654312902 shares, No referral fee
-- Swapped 26433575129761361463 aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near for 1233967190597137433407275 wrap.near
-- Exchange v2.ref-finance.near got 57194534600681424 shares, No referral fee
