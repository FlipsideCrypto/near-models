{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  unique_key = ['tx_hash', 'receipt_id', 'log_index', 'log_event_index', 'amount_index'],
  merge_exclude_columns = ['inserted_timestamp'],
  cluster_by = ['block_timestamp::DATE'],
  incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receiver_id);",
  tags = ['intents','curated','scheduled_non_core']
) }}

with intent_txs as (
  select distinct
      tx_hash
  from {{ ref('silver__streamline_receipts_final') }}
  where block_timestamp >= '2024-11-01'
      and receiver_id = 'intents.near'
      and receipt_actions:receipt:Action:actions[0]:FunctionCall:method_name::string = 'execute_intents'
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
    {{ ref('silver__logs_s3') }} l
  join intent_txs r on r.tx_hash = l.tx_hash
  where
    receiver_id = 'intents.near'
    and block_timestamp >= '2024-11-01'
    and try_parse_json(clean_log) :standard :: string = 'nep245'
), 
flatten_logs AS (
    select
        l.block_timestamp,
        l.block_id,
        l.tx_hash,
        l.receipt_id,
        l.receiver_id,
        l.predecessor_id,
        l.log_event,
        l.gas_burnt,
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
    log_index,
    log_event_index,
    owner_id,
    old_owner_id,
    new_owner_id,
    memo,
    INDEX as amount_index,
    VALUE::string as amount_raw,
    token_ids[INDEX] AS token_id,
    gas_burnt,
    receipt_succeeded
  from
    flatten_logs,
    lateral flatten(input => amounts)
)
SELECT 
  *,
  {{
    dbt_utils.generate_surrogate_key(
      ['tx_hash', 'receipt_id', 'log_index', 'log_event_index', 'amount_index']
    )
  }} AS intent_id,
  SYSDATE() AS _inserted_timestamp,
  SYSDATE() AS _modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM flatten_arrays;


select * from near_dev.silver.intents;