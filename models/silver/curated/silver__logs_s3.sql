{{ config(
    materialized = "incremental",
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["_inserted_timestamp::DATE","block_timestamp::DATE"],
    unique_key = "log_id",
    incremental_strategy = "merge",
    tags = ['curated','scheduled_non_core']
) }}

WITH receipts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_object_id,
        logs,
        receiver_id,
        receipt_actions :predecessor_id :: STRING AS predecessor_id,
        signer_id,
        gas_burnt,
        receipt_succeeded,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }}

    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
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
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_object_id,
        concat_ws(
            '-',
            receipt_object_id,
            INDEX
        ) AS log_id,
        INDEX AS log_index,
        receiver_id,
        predecessor_id,
        signer_id,
        COALESCE(TRY_PARSE_JSON(VALUE), TRY_PARSE_JSON(SPLIT(VALUE, 'EVENT_JSON:') [1]), VALUE :: STRING) AS clean_log,
        VALUE ILIKE 'event_json:%' AS is_standard,
        gas_burnt,
        receipt_succeeded,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        receipts,
        LATERAL FLATTEN(
            input => logs
        )
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['log_id']
    ) }} AS logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
