{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::DATE', '_modified_timestamp::DATE'],
    unique_key = 'actions_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receipt_receiver_id,receipt_signer_id,receipt_predecessor_id,actions_id);",
    tags = ['actions', 'curated', 'scheduled_core', 'grail']
) }}
-- TODO: add back SO, incremental predicates
WITH transactions AS (

    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        gas_used AS tx_gas_used,
        tx_succeeded,
        _modified_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        block_timestamp > CURRENT_DATE - 3
    
    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
        WHERE modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
    {% endif %}
),
receipts AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id AS receipt_id,
        receiver_id AS receipt_receiver_id,
        signer_id AS receipt_signer_id,
        receipt_actions :predecessor_id :: STRING AS receipt_predecessor_id,
        receipt_succeeded,
        gas_burnt AS receipt_gas_burnt,
        status_value,
        receipt_actions,
        logs AS receipt_logs,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        block_timestamp > CURRENT_DATE - 30

    {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
        WHERE _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
    {% endif %}
),
join_data AS (
    SELECT
        r.block_id,
        r.block_timestamp,
        t.tx_hash,
        t.tx_signer,
        t.tx_receiver,
        t.tx_gas_used,
        t.tx_succeeded,
        r.receipt_id,
        r.receipt_receiver_id,
        r.receipt_signer_id,
        r.receipt_predecessor_id,
        r.receipt_succeeded,
        r.receipt_gas_burnt,
        r.status_value,
        r.receipt_actions,
        r.receipt_logs,
        r._partition_by_block_number,
        r._inserted_timestamp
    FROM
        transactions t
        LEFT JOIN receipts r
        ON t.tx_hash = r.tx_hash
),
flatten_actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        tx_signer,
        tx_receiver,
        tx_gas_used,
        tx_succeeded,
        receipt_id,
        receipt_receiver_id,
        receipt_signer_id,
        receipt_predecessor_id,
        receipt_succeeded,
        receipt_gas_burnt,
        IFF(
            object_keys(status_value)[0] :: STRING = 'SuccessValue', 
            OBJECT_INSERT(
                    status_value, 
                    'SuccessValue',
                    COALESCE(
                        TRY_PARSE_JSON(
                            TRY_BASE64_DECODE_STRING(
                                GET(status_value, 'SuccessValue')
                            )
                        ), 
                        GET(status_value, 'SuccessValue')
                    ),
                    TRUE 
                ), 
            status_value) as receipt_status_value,
        receipt_logs, -- TODO review logs, clean logs?
        False AS is_delegated,
        INDEX AS action_index,
        receipt_actions :receipt :Action :gas_price :: NUMBER AS action_gas_price,
        IFF(VALUE = 'CreateAccount', VALUE, object_keys(VALUE) [0] :: STRING) AS action_name,
        IFF(
            VALUE = 'CreateAccount',
            {},
            GET(VALUE, object_keys(VALUE) [0] :: STRING)
        ) AS action_data,
        IFF(
            action_name = 'FunctionCall',
            OBJECT_INSERT(
                action_data,
                'args',
                COALESCE(
                    TRY_PARSE_JSON(
                        TRY_BASE64_DECODE_STRING(
                            action_data :args :: STRING
                        )
                    ),
                    action_data :args
                ),
                TRUE
            ),
            action_data
        ) AS action_data_parsed,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        join_data,
        LATERAL FLATTEN(
            receipt_actions :receipt :Action :actions :: ARRAY
        )
),
flatten_delegated_actions AS (
    SELECT
        tx_hash,
        True AS is_delegated,
        object_keys(VALUE)[0] ::STRING AS delegated_action_name,
        IFF(
            VALUE = 'CreateAccount',
            {},
            GET(VALUE, object_keys(VALUE) [0] :: STRING)
        ) AS delegated_action_data
    FROM flatten_actions, LATERAL FLATTEN(action_data :delegate_action :actions :: ARRAY)
    WHERE action_name = 'Delegate'
)
SELECT
    block_id,
    block_timestamp,
    fa.tx_hash,
    tx_succeeded,
    tx_receiver,
    tx_signer,
    tx_gas_used,
    fa.receipt_id,
    receipt_predecessor_id,
    receipt_receiver_id,
    receipt_signer_id,
    receipt_succeeded,
    receipt_gas_burnt,
    receipt_status_value,
    action_index,
    action_name,
    action_data_parsed AS action_data,
    action_gas_price,
    receipt_logs,
    _partition_by_block_number,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_actions fa
    LEFT JOIN flatten_delegated_actions da
    ON fa.tx_hash = da.tx_hash
    AND fa.action_name = da.delegated_action_name
    AND fa.action_data :: STRING = da.delegated_action_data :: STRING
