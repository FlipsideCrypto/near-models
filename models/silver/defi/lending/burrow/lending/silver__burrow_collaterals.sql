{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "_action_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH
actions AS (

    SELECT
        action_id AS _action_id,
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
    {% if is_incremental() %}
    WHERE
         {{ incremental_load_filter('_modified_timestamp') }}
    {% endif %}
),
FINAL AS (
    SELECT
        *,
        args :sender_id AS sender,
        receiver_id AS contract_address,
        CASE 
            WHEN method_name = 'ft_on_transfer' THEN PARSE_JSON(SUBSTRING(logs [1], 12))
            WHEN method_name = 'oracle_on_call' THEN PARSE_JSON(SUBSTRING(logs [0], 12))
        END AS segmented_data,
        segmented_data :data [0] :account_id AS account_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount,
        segmented_data :event :: STRING AS actions
    FROM actions
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND receipt_succeeded = TRUE
        AND (
        (method_name = 'ft_on_transfer'
        AND args:msg != ''
        AND actions = 'increase_collateral') -- increase_collateral
            OR
        (method_name = 'oracle_on_call'
        AND actions = 'decrease_collateral') -- decrease_collateral
        )
    )
SELECT
    _action_id,
    tx_hash,
    block_id AS block_number,
    block_timestamp,
    sender,
    actions,
    contract_address,
    amount,
    token_contract_address,
    _inserted_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['_action_id']
    ) }} AS actions_events_addkey_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
