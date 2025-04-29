{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'token_transfer_deposit_id',
    tags = ['curated','scheduled_non_core']
) }}

WITH transfers AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_id,
        action_index,
        receipt_predecessor_id AS predecessor_id,
        receipt_signer_id AS signer_id,
        receipt_receiver_id AS receiver_id,
        action_data :deposit :: INT AS amount_unadj,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'FunctionCall'
        AND receipt_succeeded
        AND action_data :deposit :: INT > 0

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
    receipt_id AS action_id,
    receipt_id,
    action_index,
    predecessor_id,
    receiver_id,
    amount_unadj,
    amount_unadj :: DOUBLE / pow(10, 24) AS amount_adj,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index', 'predecessor_id', 'receiver_id', 'amount_unadj']
    ) }} AS token_transfer_deposit_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transfers
