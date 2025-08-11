{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'mint_id',
    tags = ['scheduled_non_core']
) }}

WITH ft_mint_logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receiver_id AS contract_address,
        predecessor_id,
        signer_id,
        log_index,
        try_parse_json(clean_log) AS log_data,
        receipt_succeeded,
        _partition_by_block_number
    FROM 
        {{ ref('silver__logs_s3') }}
    WHERE 
        is_standard -- Only look at EVENT_JSON formatted logs
        AND try_parse_json(clean_log) :event :: STRING = 'ft_mint'

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
ft_mints_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        contract_address,
        predecessor_id,
        signer_id,
        NVL(
            f.value :old_owner_id,
            NULL
        ) :: STRING AS from_address,
        COALESCE(
            f.value :new_owner_id,
            f.value :owner_id,
            f.value :user_id,
            f.value :accountId
        ) :: STRING AS to_address,
        f.value :amount :: STRING AS amount_unadj,
        f.value :memo :: STRING AS memo,
        log_index + f.index AS event_index,
        _partition_by_block_number,
        receipt_succeeded
    FROM
        ft_mint_logs,
        LATERAL FLATTEN(
            input => log_data :data
        ) f
    WHERE
        amount_unadj :: DOUBLE > 0
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id AS action_id,
    receipt_id,
    contract_address,
    from_address,
    to_address,
    amount_unadj,
    memo,
    event_index AS rn,
    predecessor_id,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'contract_address', 'amount_unadj', 'to_address', 'rn']
    ) }} AS mint_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ft_mints_final
