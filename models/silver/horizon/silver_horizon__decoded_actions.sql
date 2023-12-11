{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'action_id_horizon',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['curated', 'horizon']
) }}

WITH all_horizon_receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver_horizon__receipts') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
decoded_function_calls AS (
    SELECT
        *,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
        AND _partition_by_block_number >= 85000000
        AND SPLIT(
            action_id,
            '-'
        ) [0] :: STRING IN (
            SELECT
                DISTINCT receipt_object_id
            FROM
                all_horizon_receipts
        )
),
FINAL AS (
    SELECT
        fc.action_id,
        fc.tx_hash,
        r.receipt_object_id,
        fc.block_id,
        fc.block_timestamp,
        fc.method_name,
        fc.args,
        fc.deposit,
        fc.attached_gas,
        r.receiver_id,
        r.signer_id,
        r.receipt_succeeded,
        r.logs,
        fc._load_timestamp,
        fc._partition_by_block_number,
        fc._inserted_timestamp
    FROM
        decoded_function_calls fc
        LEFT JOIN all_horizon_receipts r USING (receipt_object_id)
)
SELECT
    action_id AS action_id_horizon,
    tx_hash,
    receipt_object_id,
    block_id,
    block_timestamp,
    method_name,
    args,
    deposit,
    attached_gas,
    receiver_id,
    signer_id,
    receipt_succeeded,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id_horizon']
    ) }} AS horizon_decoded_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
