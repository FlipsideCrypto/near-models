{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH --borrows from Burrow LendingPool contracts
borrow AS (

    SELECT
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
        PARSE_JSON(
            args :msg
        ) :Execute :actions [0] :Borrow AS segmented_data,
        segmented_data :token_id AS token_contract_address,
        segmented_data :amount AS borrowed_amount
    FROM
        borrow
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND method_name = 'oracle_on_call'
        AND segmented_data IS NOT NULL
        AND receipt_succeeded = TRUE
)
SELECT
    tx_hash,
    block_id,
    block_timestamp,
    receiver_id AS contract_address,
    sender,
    token_contract_address,
    amount,
    _inserted_timestamp,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS actions_events_addkey_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
