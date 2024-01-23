{{ config(
  materialized = 'incremental',
  incremental_strategy = 'merge',
  merge_exclude_columns = ["inserted_timestamp"],
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
  unique_key = 'action_id',
  tags = ['actions', 'curated']
) }}

WITH action_events AS (

  SELECT
    *
  FROM
    {{ ref('silver__actions_events_s3') }}
  WHERE
    action_name = 'FunctionCall' {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
      AND {{ incremental_load_filter('_inserted_timestamp') }}
    {% endif %}
),
FINAL AS (
  SELECT
    action_id,
    tx_hash,
    receiver_id,
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
    _partition_by_block_number,
    _inserted_timestamp,
    _load_timestamp,
    {{ dbt_utils.generate_surrogate_key(
      ['action_id']
    ) }} AS actions_events_function_call_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
  FROM
    action_events
)
SELECT
  *
FROM
  FINAL
