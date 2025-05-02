{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_borrows_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH --borrows from Burrow LendingPool contracts
borrows AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_predecessor_id AS predecessor_id,
        receipt_receiver_id AS receiver_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        receipt_succeeded,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_receiver_id = 'contract.main.burrow.near'
        AND action_name = 'FunctionCall'
        AND receipt_succeeded
        AND action_data :method_name :: STRING = 'oracle_on_call'

        {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
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
        *,
        args :sender_id :: STRING AS sender_id,
        receiver_id AS contract_address,
        PARSE_JSON(
            args :msg
        ) :Execute :actions [0] : Borrow :: OBJECT  AS segmented_data,
        segmented_data :token_id AS token_contract_address,
        COALESCE( segmented_data :amount,segmented_data :max_amount)  AS amount_raw,
        'borrow' :: STRING AS actions
    FROM
        borrows
    WHERE
        segmented_data IS NOT NULL
)
SELECT
    tx_hash,
    block_id,
    block_timestamp,
    receipt_id,
    action_index,
    predecessor_id,
    sender_id,
    actions,
    contract_address,
    amount_raw,
    token_contract_address,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS burrow_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    actions != 'fee_detail'
