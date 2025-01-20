with intent_receipts AS (
    select 
        tx_hash,
        block_timestamp,
        block_id,
        receipt_object_id,
        receiver_id,
        predecessor_id,
        signer_id,
        receipt_actions:receipt:Action:actions[0]:FunctionCall:method_name::string as method_name,
        receipt_actions:receipt:Action:actions[0]:FunctionCall:args::string as function_args,
        receipt_actions:receipt:Action:actions[0]:Transfer:deposit::string as transfer_deposit,
        case 
            when receipt_actions:receipt:Action:actions[0]:Transfer is not null then 'Transfer'
            when receipt_actions:receipt:Action:actions[0]:FunctionCall is not null then 'FunctionCall'
            else null
        end as action_name,
        receipt_succeeded
    from near.silver.streamline_receipts_final 
    where block_timestamp >= '2024-11-01'
        and receiver_id = 'intents.near'
        and (
            receipt_actions:receipt:Action:actions[0]:FunctionCall:method_name::string = 'execute_intents'
            or receipt_actions:receipt:Action:actions[0]:Transfer is not null
        )
    qualify count(*) over (partition by tx_hash) > 1
),
intent_txs AS (
  select distinct tx_hash from intent_receipts
),
nep245_logs AS (
select
    block_timestamp,
    block_id,
    l.tx_hash,
    receipt_object_id AS receipt_id,
    log_index,
    receiver_id,
    predecessor_id,
    signer_id,
    gas_burnt,
    try_parse_json(clean_log) :event :: string as log_event,
    try_parse_json(clean_log) :data :: array as log_data,
    array_size(log_data) as log_data_len,
    receipt_succeeded
  from
    near.core.fact_logs l
  join intent_txs r on r.tx_hash = l.tx_hash
  where
    receiver_id = 'intents.near'
    and block_timestamp >= '2024-11-01'
    and try_parse_json(clean_log) :standard :: string = 'nep245'
), flatten_logs AS (
    select
        l.block_timestamp,
        l.block_id,
        l.tx_hash,
        l.receipt_id,
        l.receiver_id,
        l.predecessor_id,
        l.log_event,
        THIS as log_event_this,
        INDEX as log_event_index,
        VALUE:amounts::array as amounts,
        VALUE:token_ids::array as token_ids,
        VALUE:owner_id::string as owner_id,
        VALUE:old_owner_id::string as old_owner_id,
        VALUE:new_owner_id::string as new_owner_id,
        VALUE:memo::string as memo,
        l.log_index,
        l.receipt_succeeded,
        array_size(amounts) as amounts_size,
        array_size(token_ids) as token_ids_size
    from nep245_logs l,
    lateral flatten(input => log_data)
), flatten_arrays AS (
  select
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    log_index,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    INDEX as amount_index,
    VALUE::string as amount,
    token_ids[INDEX] AS token_id,
    receipt_succeeded
  from
    flatten_logs,
    lateral flatten(input => amounts)
)
select * from flatten_arrays sample (25 rows);