{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'actions_id'
) }}
-- TODO: add back cluster, SO, incremental predicates, tags
WITH transactions AS (

    SELECT
        tx_hash,
        tx_signer,
        tx_receiver,
        gas_used AS tx_gas_used,
        tx_succeeded
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        block_timestamp > CURRENT_DATE - 30
    -- TODO add incremental filter
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
        receipt_actions,
        logs AS receipt_logs,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        block_timestamp > CURRENT_DATE - 30
    -- TODO add incremental filter
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
        receipt_logs, -- TODO review logs, clean logs?
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
)
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
    receipt_logs,
    action_index,
    action_gas_price,
    action_name,
    action_data_parsed AS action_data,
    _partition_by_block_number,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_actions
