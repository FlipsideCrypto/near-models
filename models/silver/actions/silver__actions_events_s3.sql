{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    unique_key = 'action_id',
    tags = ['actions', 'curated']
) }}

WITH receipts AS (

    SELECT
        tx_hash,
        receipt_object_id,
        receiver_id,
        signer_id,
        block_id,
        block_timestamp,
        chunk_hash,
        logs,
        receipt_actions,
        execution_outcome,
        receipt_succeeded,
        gas_burnt,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            WHERE _modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
),
flatten_actions AS (
    SELECT
        tx_hash,
        receipt_object_id,
        receiver_id,
        signer_id,
        block_id,
        block_timestamp,
        chunk_hash,
        logs,
        receipt_actions,
        receipt_actions :predecessor_id :: STRING as predecessor_id,
        receipt_actions :receipt :Action :gas_price :: NUMBER as gas_price,
        execution_outcome,
        gas_burnt,
        execution_outcome :outcome :tokens_burnt :: NUMBER as tokens_burnt,
        VALUE AS action_object,
        INDEX AS action_index,
        receipt_succeeded,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        receipts,
        LATERAL FLATTEN(
            input => receipt_actions :receipt :Action :actions
        )
),
FINAL AS (
    SELECT
        concat_ws(
            '-',
            receipt_object_id,
            action_index
        ) AS action_id,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_object_id,
        chunk_hash,
        predecessor_id,
        receiver_id,
        signer_id,
        action_index,
        key AS action_name,
        TRY_PARSE_JSON(VALUE) AS action_data,
        logs,
        receipt_succeeded,
        gas_price,
        gas_burnt,
        tokens_burnt,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        flatten_actions,
        LATERAL FLATTEN(
            input => action_object
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'action_index']
    ) }} AS actions_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
