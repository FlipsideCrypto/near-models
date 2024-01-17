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