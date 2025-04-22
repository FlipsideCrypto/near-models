{{ config(
  materialized = 'incremental',
  merge_exclude_columns = ["inserted_timestamp"],
  incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
  cluster_by = ['block_timestamp::DATE'],
  unique_key = 'token_transfer_native_id',
  incremental_strategy = 'merge',
  tags = ['curated','scheduled_non_core']
) }}

WITH action_events AS(

  SELECT
    tx_hash,
    block_id,
    block_timestamp,
    action_id,
    action_data :deposit :: STRING AS amount_unadj,
    predecessor_id,
    receiver_id,
    signer_id,
    receipt_succeeded,
    gas_price,
    gas_burnt,
    tokens_burnt,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'Transfer' 
    AND 
      receipt_succeeded
    
    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
{% endif %}
)

SELECT
  tx_hash,
  block_id,
  block_timestamp,
  action_id,
  amount_unadj,
  amount_unadj :: DOUBLE / pow(
    10,
    24
  ) AS amount_adj,
  predecessor_id,
  receiver_id,
  signer_id,
  receipt_succeeded,
  gas_price,
  gas_burnt,
  tokens_burnt,
  _partition_by_block_number,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['action_id', 'predecessor_id', 'receiver_id', 'amount_unadj']
  ) }} AS token_transfer_native_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  action_events
