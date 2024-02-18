{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "_action_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH --borrows from Burrow LendingPool contracts
borrows AS (

    SELECT
        action_id as _action_id,
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
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
        PARSE_JSON(
            args :msg
        ) :Execute :actions [0] :Borrow AS segmented_data,
        segmented_data :token_id AS token_contract_address,
        segmented_data :amount AS amount,
        'borrow' :: STRING AS actions
    FROM
        borrows
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND method_name = 'oracle_on_call'
        AND segmented_data IS NOT NULL
        AND receipt_succeeded = TRUE
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
