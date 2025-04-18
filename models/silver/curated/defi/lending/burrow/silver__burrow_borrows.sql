{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = "burrow_borrows_id",
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,sender_id);",
    tags = ['curated','scheduled_non_core']
) }}

WITH --borrows from Burrow LendingPool contracts
borrows AS (

    SELECT
        action_id as action_id,
        block_id,
        block_timestamp,
        tx_hash,
        method_name,
        args,
        receiver_id,
        receipt_succeeded,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receiver_id = 'contract.main.burrow.near'
        AND method_name = 'oracle_on_call'
        AND receipt_succeeded = TRUE

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
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id']
    ) }} AS burrow_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    actions != 'fee_detail'
