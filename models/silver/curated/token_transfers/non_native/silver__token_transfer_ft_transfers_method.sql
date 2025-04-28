{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'transfers_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH ft_transfer_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_receiver_id AS contract_address,
        receipt_predecessor_id AS predecessor_id,
        receipt_signer_id AS signer_id,
        receipt_succeeded,
        _partition_by_block_number
    FROM 
        {{ ref('core__ez_actions') }}
    WHERE 
        action_name = 'FunctionCall'
        AND action_data :method_name :: STRING = 'ft_transfer'
        AND receipt_succeeded

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
ft_transfer_logs AS (
    SELECT
        l.block_id,
        l.block_timestamp,
        l.tx_hash,
        l.receipt_id,
        l.log_index,
        l.clean_log AS log_value,
        l._partition_by_block_number,
        a.contract_address,
        a.predecessor_id,
        a.signer_id,
        a.action_index
    FROM
        {{ ref('silver__logs_s3') }} l
        INNER JOIN ft_transfer_actions a
        ON l.tx_hash = a.tx_hash 
        AND l.receipt_id = a.receipt_id
    WHERE
        l.receipt_succeeded
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
        REGEXP_SUBSTR(
            log_value,
            'from ([^ ]+)',
            1,
            1,
            '',
            1
        ) :: STRING AS from_address,
        REGEXP_SUBSTR(
            log_value,
            'to ([^ ]+)',
            1,
            1,
            '',
            1
        ) :: STRING AS to_address,
        REGEXP_SUBSTR(
            log_value,
            '\\d+'
        ) :: variant AS amount_unadj,
        '' AS memo,
        log_index + action_index AS event_index,
        _partition_by_block_number
    FROM
        ft_transfer_logs
    WHERE
        from_address IS NOT NULL
        AND to_address IS NOT NULL
        AND amount_unadj IS NOT NULL
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    contract_address,
    predecessor_id,
    signer_id,
    from_address,
    to_address,
    amount_unadj,
    memo,
    event_index,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'contract_address', 'amount_unadj', 'from_address', 'to_address', 'event_index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ft_transfers_final
