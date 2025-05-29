{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'token_transfer_native_id',
    tags = ['scheduled_non_core']
) }}

WITH transfers AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_id,
        action_index,
        action_data :deposit :: STRING AS amount_unadj,
        receipt_predecessor_id AS predecessor_id,
        receipt_receiver_id AS receiver_id,
        receipt_signer_id AS signer_id,
        receipt_succeeded,
        action_gas_price AS gas_price,
        receipt_gas_burnt AS gas_burnt,
        receipt_gas_burnt * action_gas_price / pow(10, 24) AS tokens_burnt,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        action_name = 'Transfer'

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
    amount_unadj,
    amount_unadj :: DOUBLE / pow(10, 24) AS amount_adj,
    predecessor_id,
    receiver_id,
    receipt_succeeded,
    gas_price,
    gas_burnt,
    tokens_burnt,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index', 'predecessor_id', 'receiver_id', 'amount_unadj']
    ) }} AS token_transfer_native_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    transfers
