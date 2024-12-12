{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::Date'],
    unique_key = 'transfers_base_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH nep141 AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        predecessor_id,
        signer_id,
        receiver_id,
        action_name,
        method_name,
        deposit,
        logs,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        AND RIGHT(
            action_id,
            2
        ) = '-0' 
    {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
    {% elif is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)
SELECT
    *,
  {{ dbt_utils.generate_surrogate_key(
    ['action_id']
  ) }} AS transfers_base_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
    nep141
