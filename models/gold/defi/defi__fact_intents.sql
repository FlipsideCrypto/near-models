with intent_txs as (
    select distinct 
        tx_hash
    from near.core.ez_actions
    where receipt_receiver_id = 'intents.near'
        and action_name = 'FunctionCall'
        and action_data:method_name::string = 'execute_intents'
        and block_timestamp >= '2024-11-01'
        and block_timestamp < current_date() - 7
),
with nep245_logs as (
  select
    block_timestamp,
    block_id,
    tx_hash,
    receipt_object_id AS receipt_id,
    receiver_id,
    predecessor_id,
    try_parse_json(clean_log) :event :: STRING as log_event,
    try_parse_json(clean_log) :data :: ARRAY as log_data,
    array_size(log_data) as log_data_len,
    log_index,
    receipt_succeeded
  from
    near.core.fact_logs
  join intent_txs i
    on l.tx_hash = i.tx_hash
  where
    receiver_id = 'intents.near'
    and block_timestamp >= '2024-11-01'
    and try_parse_json(clean_log) :standard :: STRING = 'nep245'
),
flatten_logs AS (
  SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    THIS AS log_event_this,
    INDEX AS log_event_index,
    VALUE :amounts :: ARRAY as amounts,
    VALUE :token_ids :: ARRAY as token_ids,
    VALUE :owner_id :: STRING as owner_id,
    VALUE :old_owner_id :: STRING as old_owner_id,
    VALUE :new_owner_id :: STRING as new_owner_id,
    VALUE :memo :: STRING as memo,
    log_index,
    receipt_succeeded
  FROM
    nep245_logs,
    lateral flatten(input => log_data)
),
flatten_arrays AS (
  select
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    amounts.INDEX as amount_index,
    amounts.VALUE as amount,
    token_ids [INDEX] AS token_id,
    log_index,
    receipt_succeeded
  from
    flatten_logs,
    lateral flatten(input => amounts) amounts
),
parse_logs as (
  SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    amount as token_amount_unadj,
    token_id as token_id_raw,
    COALESCE(
      UPPER(
        REGEXP_SUBSTR(token_id_raw, '^nep141:([a-z]+)-', 1, 1, 'e', 1)
      ),
      'NEAR'
    ) as blockchain,
    IFF(
      REGEXP_SUBSTR(token_id_raw, '^nep141:([a-z]+)-', 1, 1, 'e', 1) is null,
      SPLIT(token_id_raw, ':') [1],
      SPLIT(SPLIT(token_id_raw, ':') [1], '-') [1]
    ) as token_address_a,
    IFF(
      token_address_a like '%.omft.near',
      SPLIT(token_address_a, '.') [0],
      token_address_a
    ) as token_address,
    log_index,
    receipt_succeeded
  FROM
    flatten_arrays
)
select
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    receiver_id,
    predecessor_id,
    log_event,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    token_amount_unadj,
    token_id_raw,
    CASE
      blockchain
      WHEN 'ETH' THEN 'ethereum'
      WHEN 'ARB' THEN 'arbitrum'
      ELSE lower(blockchain)
    END AS blockchain,
    COALESCE(
      lower(B.token_address_fixed),
      lower(A.token_address)
    ) AS token_address,
    log_index,
    receipt_succeeded;