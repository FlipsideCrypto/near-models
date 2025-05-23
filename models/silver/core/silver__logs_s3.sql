{{ config(
    materialized = "incremental",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    cluster_by = ["block_timestamp::DATE","modified_timestamp::DATE"],
    unique_key = "log_id",
    incremental_strategy = "merge",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id);",
    tags = ['scheduled_core']
) }}

WITH receipts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        outcome_json :outcome :logs AS logs,
        receiver_id,
        predecessor_id,
        receipt_json :receipt :Action :signer_id :: STRING AS signer_id,
        outcome_json :outcome :gas_burnt AS gas_burnt,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        {{ ref('silver__receipts_final') }}

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
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        concat_ws(
            '-',
            receipt_id,
            INDEX
        ) AS log_id,
        INDEX AS log_index,
        receiver_id,
        predecessor_id,
        signer_id,
        COALESCE(
            TRY_PARSE_JSON(VALUE), 
            TRY_PARSE_JSON(SPLIT(VALUE, 'EVENT_JSON:') [1]),
            VALUE :: STRING
        ) AS clean_log,
        VALUE ILIKE 'event_json:%' AS is_standard,
        gas_burnt,
        receipt_succeeded,
        _partition_by_block_number
    FROM
        receipts,
        LATERAL FLATTEN(
            input => logs
        )
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    log_id,
    log_index,
    receiver_id,
    predecessor_id,
    signer_id,
    clean_log,
    is_standard,
    gas_burnt,
    receipt_succeeded,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['log_id']
    ) }} AS logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
