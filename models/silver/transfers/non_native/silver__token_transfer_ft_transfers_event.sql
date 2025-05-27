{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'transfers_event_id',
    tags = ['scheduled_non_core']
) }}

WITH ft_transfer_logs AS (
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
        AND try_parse_json(clean_log) :event :: STRING = 'ft_transfer'

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
ft_transfers_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        contract_address,
        predecessor_id,
        signer_id,
        log_index,
        NVL(
            f.value :old_owner_id,
            NULL
        ) :: STRING AS from_address,
        NVL(
            f.value :new_owner_id,
            f.value :owner_id
        ) :: STRING AS to_address,
        f.value :amount :: variant AS amount_unadj,
        f.value :memo :: STRING AS memo,
        log_index + f.index AS event_index,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        ft_transfer_logs,
        LATERAL FLATTEN(
            input => log_data :data
        ) f
    WHERE
        amount_unadj :: INT > 0
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id AS action_id,
    receipt_id,
    contract_address,
    predecessor_id,
    from_address,
    to_address,
    amount_unadj,
    memo,
    event_index AS rn,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'contract_address', 'amount_unadj', 'from_address', 'to_address', 'rn']
    ) }} AS transfers_event_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ft_transfers_final
