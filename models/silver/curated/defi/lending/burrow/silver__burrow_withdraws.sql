{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_withdraws_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH -- successfull withdraws
withdraws AS (

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
        AND method_name = 'after_ft_transfer'
        AND receipt_succeeded = TRUE
    {% if is_incremental() %}
        AND {{ incremental_load_filter('_modified_timestamp') }}
    {% endif %}
),
FINAL AS (
    SELECT
        *,
        receiver_id AS contract_address,
        PARSE_JSON(SUBSTRING(logs [0], 12)) :: OBJECT AS segmented_data,
        segmented_data :data [0] :account_id AS sender_id,
        segmented_data :data [0] :token_id AS token_contract_address,
        segmented_data :data [0] :amount :: NUMBER AS amount_raw,
        segmented_data :event :: STRING AS actions
    FROM
        withdraws
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
    ) }} AS burrow_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL