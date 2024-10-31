{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'aurora_encoded_withdraw_receipts_id',
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['curated','scheduled_non_core', 'grail'],
) }}

WITH receipts AS (

    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        receipt_object_id,
        signer_id,
        receiver_id,
        receipt_actions :predecessor_id :: STRING AS predecessor_id,
        receipt_actions :receipt :Action :actions [0] :FunctionCall :args :: STRING AS encoded_input,
        status_value :SuccessValue :: STRING AS encoded_output
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        signer_id = 'relay.aurora'
        AND object_keys(
            receipt_actions :receipt :Action :actions [0]
        ) [0] = 'FunctionCall'
        AND receipt_actions :receipt :Action :actions [0] :FunctionCall :method_name :: STRING = 'withdraw' 
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
    receipt_object_id,
    signer_id,
    receiver_id,
    predecessor_id,
    encoded_input,
    encoded_output,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id']
    ) }} AS aurora_encoded_withdraw_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    receipts
