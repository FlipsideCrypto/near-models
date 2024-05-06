{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_collaterals_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH
actions AS (

    SELECT
        action_id AS action_id,
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        logs,
        receiver_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND receipt_succeeded = TRUE

        {% if var("MANUAL_FIX") %}
        AND 
        {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if is_incremental() %}
        
            AND _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        {% endif %}
),
FINAL AS (
    SELECT
        *,
        args :sender_id:: STRING AS sender_id,
        receiver_id AS contract_address,
        CASE 
            WHEN method_name = 'ft_on_transfer' THEN PARSE_JSON(SUBSTRING(logs [1], 12))
            WHEN method_name = 'oracle_on_call' THEN PARSE_JSON(SUBSTRING(logs [0], 12))
        END :: OBJECT  AS segmented_data,
        segmented_data :data [0] :account_id AS account_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount_raw,
        segmented_data :event :: STRING AS actions
    FROM 
        actions
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
    action_id,
    tx_hash,
    block_id,
    block_timestamp,
    sender_id,
    actions,
    contract_address,
    amount_raw,
    token_contract_address,
    _inserted_timestamp,
    _modified_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id']
    ) }} AS burrow_collaterals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
