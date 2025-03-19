-- TODO slated for deprecation and drop
-- Note - must migrate curated to new ez_actions first
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'action_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,signer_id,receipt_object_id,receiver_id);",
    tags = ['actions', 'curated','scheduled_core', 'grail']
) }}
-- todo deprecate this model
WITH receipts AS (

    SELECT
        tx_hash,
        receipt_id AS receipt_object_id,
        receiver_id,
        receipt_json :receipt :Action :signer_id :: STRING AS signer_id,
        block_id,
        block_timestamp,
        chunk_hash,
        outcome_json :outcome :logs :: ARRAY AS logs,
        receipt_json AS receipt_actions,
        outcome_json AS execution_outcome,
        receipt_succeeded,
        outcome_json :outcome :gas_burnt :: NUMBER AS gas_burnt,
        _partition_by_block_number
    FROM
        {{ ref('silver__receipts_final') }}
    WHERE
        block_id IS NOT NULL
        {% if var("MANUAL_FIX") %}
        AND
            {{ partition_load_manual('no_buffer') }}
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
        _partition_by_block_number
    FROM
        receipts,
        LATERAL FLATTEN(
            input => receipt_actions :receipt :Action :actions :: ARRAY
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
        _partition_by_block_number
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
