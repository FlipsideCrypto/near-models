{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'transfers_id',
    incremental_strategy = 'merge',
    tags = ['scheduled_non_core']
) }}


{% if execute %}

    {% if is_incremental() and not var("MANUAL_FIX") %}
    {% do log("Incremental and not MANUAL_FIX", info=True) %}
    {% set max_mod_query %}

    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}

    {% endset %}

        {% set max_mod = run_query(max_mod_query) [0] [0] %}
        {% if not max_mod or max_mod == 'None' %}
            {% set max_mod = '2099-01-01' %}
        {% endif %}

        {% do log("max_mod: " ~ max_mod, info=True) %}

        {% set min_block_date_query %}
    SELECT
        MIN(
            block_timestamp :: DATE
        )
    FROM
        (
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('core__ez_actions') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
            UNION ALL
            SELECT
                MIN(block_timestamp) block_timestamp
            FROM
                {{ ref('silver__logs_s3') }} A
            WHERE
                modified_timestamp >= '{{max_mod}}'
        ) 
    {% endset %}

        {% set min_bd = run_query(min_block_date_query) [0] [0] %}
        {% if not min_bd or min_bd == 'None' %}
            {% set min_bd = '2099-01-01' %}
        {% endif %}

        {% do log("min_bd: " ~ min_bd, info=True) %}

    {% endif %}

{% endif %}

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
        action_data :method_name :: STRING AS method_name,
        _partition_by_block_number,
        modified_timestamp
    FROM 
        {{ ref('core__ez_actions') }}
    WHERE 
        action_name = 'FunctionCall'
        AND action_data :method_name :: STRING in ('ft_transfer', 'ft_transfer_call')

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND block_timestamp :: DATE >= '{{min_bd}}'
        {% endif %}
    {% endif %}
),
logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        log_index,
        clean_log AS log_value,
        _partition_by_block_number,
        modified_timestamp
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        NOT is_standard
   {% if var("MANUAL_FIX") %}
        AND
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
            AND block_timestamp :: DATE >= '{{min_bd}}'
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
        l.log_value,
        l._partition_by_block_number,
        a.contract_address,
        a.predecessor_id,
        a.signer_id,
        a.action_index,
        a.receipt_succeeded,
        a.method_name
    FROM
        logs l
    INNER JOIN ft_transfer_actions a
        ON l.tx_hash = a.tx_hash 
        AND l.receipt_id = a.receipt_id
    {% if is_incremental() and not var("MANUAL_FIX") %}
        WHERE
            GREATEST(
                COALESCE(l.modified_timestamp, '1970-01-01'),
                COALESCE(a.modified_timestamp, '1970-01-01')   
            ) >= '{{max_mod}}'
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
        ) :: STRING AS amount_unadj,
        '' AS memo,
        log_index + action_index AS event_index,
        _partition_by_block_number,
        receipt_succeeded,
        method_name
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
    receipt_id AS action_id,
    receipt_id,
    contract_address,
    predecessor_id,
    signer_id,
    from_address,
    to_address,
    amount_unadj,
    memo,
    event_index AS rn,
    method_name,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'contract_address', 'amount_unadj', 'from_address', 'to_address', 'rn']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    ft_transfers_final
