{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::DATE', '_modified_timestamp::DATE'],
    unique_key = 'complete_actions_events_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,action_id,signer_id,receipt_object_id,receiver_id);",
    tags = ['actions', 'curated','scheduled_core', 'grail']
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
tx_data AS (
    SELECT
    tx_hash,
    tx_receiver,
    tx_signer,
    gas_used,
    transaction_fee,
    attached_gas,
    tx_succeeded
    FROM
        {{ ref('silver__streamline_transactions_final') }}
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
        receipt_actions :predecessor_id :: STRING AS predecessor_id,
        receipt_actions :receipt :Action :gas_price :: NUMBER AS gas_price,
        execution_outcome,
        gas_burnt,
        execution_outcome :outcome :tokens_burnt :: NUMBER AS tokens_burnt,
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
        f.tx_hash,
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
        tx_receiver,
        tx_signer,
        gas_used,
        transaction_fee,
        attached_gas,
        tx_succeeded,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        flatten_actions f
        INNER JOIN tx_data t ON f.tx_hash = t.tx_hash,
        LATERAL FLATTEN(
            input => f.action_object
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'action_index']
    ) }} AS complete_actions_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
