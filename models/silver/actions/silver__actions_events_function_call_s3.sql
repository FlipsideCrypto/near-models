-- TODO slated for deprecation and drop
-- Note - must migrate curated to new ez_actions first

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
  unique_key = 'action_id',
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(action_id,tx_hash,receiver_id,predecessor_id,signer_id,method_name);",
  tags = ['actions', 'curated','scheduled_core', 'grail']
) }}
-- todo deprecate this model
WITH action_events AS (

  SELECT
    action_id,
    tx_hash,
    receiver_id,
    predecessor_id,
    signer_id,
    block_id,
    block_timestamp,
    action_name,
    action_data,
    logs,
    receipt_succeeded,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'FunctionCall' 

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
),
FINAL AS (
  SELECT
    action_id,
    tx_hash,
    receiver_id,
    predecessor_id,
    signer_id,
    block_id,
    block_timestamp,
    action_name,
    action_data :method_name :: STRING AS method_name,
    COALESCE(
      TRY_PARSE_JSON(TRY_BASE64_DECODE_STRING(action_data :args)),
      action_data :args
    ) AS args,
    action_data :deposit :: NUMBER AS deposit,
    action_data :gas :: NUMBER AS attached_gas,
    logs,
    receipt_succeeded,
    _partition_by_block_number,
    _inserted_timestamp
  FROM
    action_events
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['action_id']
  ) }} AS actions_events_function_call_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL
