{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'token_transfer_deposit_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH functioncalls AS (

    SELECT
        action_id,
        tx_hash,
        block_id,
        block_timestamp,
        predecessor_id,
        signer_id,
        receiver_id,
        deposit,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        deposit :: INT > 0
        AND receipt_succeeded
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
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    predecessor_id,
    signer_id,
    receiver_id,
    deposit AS amount_unadj,
    deposit :: DOUBLE / pow(
        10,
        24
    ) AS amount_adj,
    receipt_succeeded,
    _partition_by_block_number,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'predecessor_id', 'receiver_id', 'amount_unadj']
    ) }} AS token_transfer_deposit_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    functioncalls
