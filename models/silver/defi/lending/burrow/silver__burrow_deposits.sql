{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_deposits_id",
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_predecessor_id AS predecessor_id,
        receipt_receiver_id AS receiver_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args AS args,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_receiver_id = 'contract.main.burrow.near'
        AND action_name = 'FunctionCall'
        AND action_data :method_name :: STRING = 'ft_on_transfer'
        AND receipt_succeeded = TRUE

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
        TRY_PARSE_JSON(clean_log) AS parsed_log
    FROM {{ ref('silver__logs_s3') }}
    WHERE 
        receiver_id = 'contract.main.burrow.near'
        AND receipt_id IN (SELECT receipt_id FROM actions)
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
        a.*,
        args :sender_id:: STRING AS sender_id,
        receiver_id AS contract_address,
        l.parsed_log AS segmented_data,
        segmented_data :data [0] :account_id AS account_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount_raw,
        segmented_data :event :: STRING AS actions
    FROM
        actions a
        LEFT JOIN logs l
        ON a.tx_hash = l.tx_hash
        AND a.receipt_id = l.receipt_id
    WHERE
        actions in ('deposit', 'deposit_to_reserve')
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
    ) }} AS burrow_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    actions not in ('fee_detail', 'margin_benefits')
