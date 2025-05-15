{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'transfers_wrapped_near_id',
    incremental_strategy = 'merge',
    tags = ['scheduled_non_core']
) }}

WITH actions AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receipt_receiver_id AS contract_address,
        IFF(action_data :method_name :: STRING = 'near_deposit', receipt_receiver_id, receipt_predecessor_id) AS from_address,
        IFF(action_data :method_name :: STRING = 'near_deposit', receipt_predecessor_id, receipt_receiver_id) AS to_address,
        action_data :method_name :: STRING AS method_name,
        COALESCE(
            action_data :args :amount,
            action_data :deposit
        ) :: STRING AS amount_unadj,
        NULL AS memo,
        action_index AS rn,
        FLOOR(
            block_id,
            -3
        ) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
        AND receipt_receiver_id = 'wrap.near'
        AND receipt_succeeded
        AND action_data :method_name :: STRING IN (
            'near_deposit',
            'near_withdraw'
        )
    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual(
            'no_buffer',
            'floor(block_id, -3)'
        ) }}
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
    block_id,
    block_timestamp,
    tx_hash,
    receipt_id,
    contract_address,
    method_name,
    from_address,
    to_address,
    amount_unadj,
    memo,
    rn,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'amount_unadj', 'from_address', 'to_address', 'rn']
    ) }} AS transfers_wrapped_near_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    actions
