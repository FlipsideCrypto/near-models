{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_collaterals_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH
actions AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_predecessor_id AS predecessor_id,
        receipt_receiver_id AS receiver_id,
        action_data :method_name :: STRING AS method_name,
        (action_data :method_name :: STRING = 'ft_on_transfer') :: INT AS target_log_index,
        action_data :args ::VARIANT AS args,
        receipt_succeeded,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_receiver_id = 'contract.main.burrow.near'
        AND action_name = 'FunctionCall'
        AND receipt_succeeded
        AND method_name in ('ft_on_transfer', 'oracle_on_call')
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
logs AS (
    SELECT
        tx_hash,
        receipt_id,
        clean_log,
        log_index
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        receiver_id = 'contract.main.burrow.near'
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
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        a.tx_hash,
        a.receipt_id,
        action_index,
        predecessor_id,
        receiver_id AS contract_address,
        args :sender_id :: STRING AS sender_id,
        method_name,
        args,
        TRY_PARSE_JSON(l.clean_log) :: OBJECT AS segmented_data,
        segmented_data :data [0] :account_id AS account_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount_raw,
        segmented_data :event :: STRING AS actions,
        _partition_by_block_number
    FROM 
        actions a
    LEFT JOIN logs l
        ON a.tx_hash = l.tx_hash
        AND a.receipt_id = l.receipt_id
        AND a.target_log_index = l.log_index
    WHERE
        (
        (method_name = 'ft_on_transfer'
        AND args:msg != ''
        AND actions = 'increase_collateral') -- increase_collateral
            OR
        (method_name = 'oracle_on_call'
        AND actions = 'decrease_collateral') -- decrease_collateral
        )
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
    ) }} AS burrow_collaterals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    actions != 'fee_detail'
