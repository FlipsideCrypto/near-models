{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'burn_id',
    tags = ['scheduled_non_core']
) }}

WITH ft_burn_logs AS (
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
        AND try_parse_json(clean_log) :event :: STRING = 'ft_burn'

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
ft_burns_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        contract_address,
        predecessor_id,
        signer_id,
        COALESCE(
            f.value :owner_id,
            f.value :account_id,
            f.value :accountId,
            NULL
        ) :: STRING AS from_address,
        NULL AS to_address,
        f.value :amount :: STRING AS amount_unadj,
        f.value :memo :: STRING AS memo,
        log_index + f.index AS event_index,
        _partition_by_block_number,
        receipt_succeeded
    FROM
        ft_burn_logs,
        LATERAL FLATTEN(
            input => log_data :data
        ) f
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
    ) }} AS burn_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ft_burns_final
