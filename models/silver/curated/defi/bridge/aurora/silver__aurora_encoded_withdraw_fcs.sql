{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'aurora_encoded_withdraw_fcs_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['curated','scheduled_non_core', 'grail'],
) }}

WITH functioncalls AS (

    SELECT
        action_id,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id,
        tx_hash,
        receiver_id,
        predecessor_id,
        signer_id,
        block_timestamp,
        block_id,
        action_name,
        method_name,
        args,
        receipt_succeeded,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receiver_id = 'aurora'
        AND method_name = 'withdraw'
        AND predecessor_id = signer_id

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
    block_timestamp,
    block_id,
    tx_hash,
    action_id,
    receipt_object_id,
    signer_id,
    receiver_id,
    predecessor_id,
    action_name,
    method_name,
    args AS encoded_input,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id']
    ) }} AS aurora_encoded_withdrawal_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    functioncalls
